use scraper::{Html, Selector};
use std::error::Error;
use std::fs::File;
use std::collections::{VecDeque, HashSet, HashMap};
use csv::Writer;
use rayon::prelude::*;
use std::sync::{Arc, Mutex};
use std::time::{Instant, Duration};
use reqwest::blocking::Client;

struct CrawlStats {
    total_urls: usize,
    successful_fetches: usize,
    failed_fetches: usize,
    total_time: Duration,
    status_codes: HashMap<u16, usize>,
    file_sizes: Vec<usize>,
    content_types: HashSet<String>,
    total_urls_extracted: usize,
    unique_urls: HashSet<String>,
    unique_urls_within: HashSet<String>,
    unique_urls_outside: HashSet<String>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let max_pages = 20_000;
    let base_domain = "https://www.latimes.com";
    let site_name = "LATimes";
    
    let stats = Arc::new(Mutex::new(CrawlStats {
        total_urls: 0,
        successful_fetches: 0,
        failed_fetches: 0,
        total_time: Duration::new(0, 0),
        status_codes: HashMap::new(),
        file_sizes: Vec::new(),
        content_types: HashSet::new(),
        total_urls_extracted: 0,
        unique_urls: HashSet::new(),
        unique_urls_within: HashSet::new(),
        unique_urls_outside: HashSet::new(),
    }));
    
    let start_url = base_domain;
    
    let fetch_writer = Arc::new(Mutex::new(Writer::from_writer(
        File::create(format!("fetch_{}.csv", site_name))?
    )));
    let visit_writer = Arc::new(Mutex::new(Writer::from_writer(
        File::create(format!("visit_{}.csv", site_name))?
    )));
    let urls_writer = Arc::new(Mutex::new(Writer::from_writer(
        File::create(format!("urls_{}.csv", site_name))?
    )));
    
    fetch_writer.lock().unwrap().write_record(&["URL", "Status"])?;
    visit_writer.lock().unwrap().write_record(&["URL", "Size (Bytes)", "Outlinks", "Content-Type"])?;
    urls_writer.lock().unwrap().write_record(&["URL", "Status"])?;

    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let visited = Arc::new(Mutex::new(HashSet::new()));
    let unique_urls = Arc::new(Mutex::new(HashSet::new()));
    {
        let mut queue = queue.lock().unwrap();
        queue.push_back(start_url.to_string());
    }

    let a_selector = Selector::parse("a").unwrap();
    let start_time = Instant::now();
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    while stats.lock().unwrap().total_urls < max_pages {
        let batch_size = 10.min(max_pages - stats.lock().unwrap().total_urls);
        let batch: Vec<String> = {
            let mut queue = queue.lock().unwrap();
            println!("Queue size: {}", queue.len());
            if queue.is_empty() {
                println!("Queue exhausted before reaching 20,000 URLs");
                break;
            }
            (0..batch_size)
                .filter_map(|_| queue.pop_front())
                .collect()
        };

        println!("Processing batch of {} URLs", batch.len());

        batch.par_iter().for_each(|url| {
            if let Err(e) = crawl_url(
                url,
                base_domain,
                &a_selector,
                &queue,
                &visited,
                &fetch_writer,
                &visit_writer,
                &urls_writer,
                &stats,
                &client,
                &unique_urls,
            ) {
                eprintln!("Error crawling {}: {}", url, e);
            }
        });
    }

    let total_time = start_time.elapsed();
    {
        let mut stats = stats.lock().unwrap();
        stats.total_time = total_time;
    }

    fetch_writer.lock().unwrap().flush()?;
    visit_writer.lock().unwrap().flush()?;
    urls_writer.lock().unwrap().flush()?;

    let stats = stats.lock().unwrap();
    println!("\nCrawl Statistics:");
    println!("Total URLs processed: {}", stats.total_urls);
    println!("Successful fetches: {}", stats.successful_fetches);
    println!("Failed fetches: {}", stats.failed_fetches);
    println!("Total time: {:.2} seconds", stats.total_time.as_secs_f64());

    println!("\nOutgoing URLs:");
    println!("Total URLs extracted: {}", stats.total_urls_extracted);
    println!("Unique URLs extracted: {}", stats.unique_urls.len());
    println!("Unique URLs within news website: {}", stats.unique_urls_within.len());
    println!("Unique URLs outside news website: {}", stats.unique_urls_outside.len());

    println!("\nStatus Codes:");
    for (code, count) in &stats.status_codes {
        println!("{}: {}", code, count);
    }

    println!("\nFile Sizes:");
    let size_ranges = vec![
        (0, 1024), // <1KB
        (1024, 10240), // 1KB-10KB
        (10240, 102400), // 10KB-100KB
        (102400, 1048576), // 100KB-1MB
        (1048576, usize::MAX), // >1MB
    ];
    for (min, max) in size_ranges {
        let count = stats.file_sizes.iter().filter(|&&size| size >= min && size < max).count();
        println!("{}-{}: {}", min, max, count);
    }

    println!("\nContent Types:");
    for content_type in &stats.content_types {
        println!("{}", content_type);
    }

    Ok(())
}

fn crawl_url(
    url: &str,
    base_domain: &str,
    selector: &Selector,
    queue: &Arc<Mutex<VecDeque<String>>>,
    visited: &Arc<Mutex<HashSet<String>>>,
    fetch_writer: &Arc<Mutex<Writer<File>>>,
    visit_writer: &Arc<Mutex<Writer<File>>>,
    urls_writer: &Arc<Mutex<Writer<File>>>,
    stats: &Arc<Mutex<CrawlStats>>,
    client: &Client,
    unique_urls: &Arc<Mutex<HashSet<String>>>,
) -> Result<(), Box<dyn Error>> {
    {
        let mut stats = stats.lock().unwrap();
        stats.total_urls += 1;
        println!("Processing URL #{}: {}", stats.total_urls, url);
    }

    {
        let mut visited = visited.lock().unwrap();
        if visited.contains(url) {
            return Ok(());
        }
        visited.insert(url.to_string());
    }

    match client.get(url).send() {
        Ok(response) => {
            let status = response.status();
            {
                let mut stats = stats.lock().unwrap();
                *stats.status_codes.entry(status.as_u16()).or_insert(0) += 1;
            }
            {
                let mut writer = fetch_writer.lock().unwrap();
                writer.write_record(&[url, &status.to_string()])?;
            }

            if status.is_success() {
                {
                    let mut stats = stats.lock().unwrap();
                    stats.successful_fetches += 1;
                }
                
                let content_type = response.headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("unknown")
                    .to_string();

                {
                    let mut stats = stats.lock().unwrap();
                    stats.content_types.insert(content_type.clone());
                }

                if let Ok(body) = response.text() {
                    let document = Html::parse_document(&body);
                    let size = body.len();
                    {
                        let mut stats = stats.lock().unwrap();
                        stats.file_sizes.push(size);
                    }
                    let outlinks: Vec<String> = document
                        .select(selector)
                        .filter_map(|element| element.value().attr("href"))
                        .map(|href| {
                            if href.starts_with("http") {
                                href.to_string()
                            } else {
                                format!("{}{}", base_domain, href)
                            }
                        })
                        .collect();

                    {
                        let mut stats = stats.lock().unwrap();
                        stats.total_urls_extracted += outlinks.len();
                    }

                    {
                        let mut writer = visit_writer.lock().unwrap();
                        writer.write_record(&[
                            url,
                            &size.to_string(),
                            &outlinks.len().to_string(),
                            &content_type,
                        ])?;
                    }

                    let mut queue = queue.lock().unwrap();
                    let visited = visited.lock().unwrap();
                    let mut unique_urls = unique_urls.lock().unwrap();
                    let mut stats = stats.lock().unwrap();
                    for link in &outlinks {
                        let status = if link.starts_with(base_domain) { "OK" } else { "N_OK" };

                        if !unique_urls.contains(link) {
                            unique_urls.insert(link.clone());
                            let mut writer = urls_writer.lock().unwrap();
                            writer.write_record(&[link, status])?;

                            if link.starts_with(base_domain) {
                                stats.unique_urls_within.insert(link.clone());
                            } else {
                                stats.unique_urls_outside.insert(link.clone());
                            }
                        }

                        if !visited.contains(link) && stats.total_urls < 20_000 {
                            queue.push_back(link.clone());
                        }
                    }
                }
            } else {
                {
                    let mut stats = stats.lock().unwrap();
                    stats.failed_fetches += 1;
                }
            }
        }
        Err(e) => {
            {
                let mut stats = stats.lock().unwrap();
                stats.failed_fetches += 1;
            }
            {
                let mut writer = fetch_writer.lock().unwrap();
                writer.write_record(&[url, &format!("Error: {}", e)])?;
            }
            println!("Failed to fetch {}: {}", url, e);
        }
    }
    Ok(())
}