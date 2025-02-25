//Nitzan Saar

/*
- Create a simple web crawler that will download webpages
                                     and gather metadata

- Measure the aspects and characteristics of the crawl


- 'Limit your crawler so it only visits HTML, doc, pdf 
        and different image format URLs and record the
                    meta data for those file types'

- The maximum pages to fetch  should be set to 20,000 to ensure a
                reasonable execution time for this exercise.
            Also, maximum depth should be set to 16 to ensure that
we limit the crawling.
- https://www.latimes.com


Crawler requirements:
1) collect information about the URLs it attempts to fetch
    - create a two column spreadsheet, column 1 containing the URL and
    column 2 containing the HTTP/HTTPS status code received;
    - fetch_NewsSite.csv
2) the files it successfully downloads, 
    - a four column spreadsheet, column 1 containing the
        URLs successfully downloaded, column 2 containing 
        the size of the downloaded file (in Bytes, or you can choose 
        your own preferred unit (bytes,kb,mb)), column 3 containing the
        # of outlinks found, and column 4 containing the resulting content-type; 
    - visit_NewsSite.csv;
3) all of the URLs (including repeats) that were discovered and processed in some way; 
    - a two column spreadsheet where column 1 contains the encountered URL and column two 
    an indicator of whether the URL a. resides in the website (OK), or b. points outside 
    of the website (N_OK). (A file points out of the website if its URL does not start with 
    the initial host/domain name, e.g. when crawling USA Today news website all inside URLs 
    must start with.)
    - urls_NewsSite.csv
4) multithreading

*/
use reqwest::blocking::get;
use scraper::{Html, Selector};
use std::error::Error;
use std::fs::File;
use std::io::Write;
use csv::Writer;

fn main() -> Result<(), Box<dyn Error>> {
    // URL of the website to scrape
    let url = "https://www.latimes.com";

    // Send a GET request to the website
    let response = get(url)?;

    // Check if the request was successful
    if !response.status().is_success() {
        eprintln!("Failed to fetch the page: {}", response.status());
        return Ok(());
    }

    // Clone the response to avoid moving it
    let status_code = response.status().to_string();
    let body = response.text()?;

    // Parse the HTML content
    let document = Html::parse_document(&body);

    // Define a CSS selector to extract specific elements
    let a_selector = Selector::parse("a").unwrap();
    let h1_selector = Selector::parse("h1").unwrap();
    let h2_selector = Selector::parse("h2").unwrap();

    // Create a CSV writer
    let file = File::create("fetch_NewsSite.csv")?;
    let mut wtr = Writer::from_writer(file);

    // Write the header
    wtr.write_record(&["URL", "Status Code"])?;

    // Iterate over the elements matching the a selector
    for element in document.select(&a_selector) {
        if let Some(href) = element.value().attr("href") {
            println!("Found link: {}", href);
            wtr.write_record(&[href, &status_code])?;
        }
    }

    // Iterate over the elements matching the h1 selector
    for element in document.select(&h1_selector) {
        let text = element.text().collect::<Vec<_>>().join(" ");
        println!("Found heading (h1): {}", text);
    }

    // Iterate over the elements matching the h2 selector
    for element in document.select(&h2_selector) {
        let text = element.text().collect::<Vec<_>>().join(" ");
        println!("Found heading (h2): {}", text);
    }

    // Flush the CSV writer to ensure all data is written
    wtr.flush()?;

    Ok(())
}
