import os
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from PyPDF2 import PdfReader
import concurrent.futures
import pandas as pd

# Function to download PDFs
def download_pdf(case_link):
    try:
        response = requests.get(case_link)
        soup = BeautifulSoup(response.text, 'html.parser')
        pdf_links = [a['href'] for a in soup.find_all('a', href=True)
                     if 'esponse' not in a['href'] and a['href'].endswith('.pdf')]
       
        for pdf_link in pdf_links:
            pdf_url = urljoin(case_link, pdf_link)
            pdf_name = os.path.basename(pdf_url)
            pdf_response = requests.get(pdf_url)
            with open(pdf_name, 'wb') as f:
                f.write(pdf_response.content)
    except Exception as e:
        print(f"Error downloading {case_link}: {e}")

# Function to process PDF and count keywords
def process_pdf(file, keywords):
    try:
        with open(file, 'rb') as f:
            reader = PdfReader(f)
            text = ""
            for page in reader.pages:
                text += page.extract_text()
       
        text = text.lower()
        text = re.sub(r'\t|\n|[ ]{2,}|[\d]|[^\w\s]', ' ', text).strip()
       
        return [len(re.findall(keyword, text)) for keyword in keywords]
    except Exception as e:
        print(f"Error processing {file}: {e}")
        return [0] * len(keywords)

# Main loop to iterate through pages
def scrape_pages(start_page, end_page):
    case_links = []
    for page_result in range(start_page, end_page + 1):
        link = f"https://www.judiciary.uk/prevention-of-future-death-reports/page/{page_result}/"
        response = requests.get(link, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(response.text, 'html.parser')
       
        page_case_links = [a['href'] for a in soup.select('.card__link')]
        case_links.extend(page_case_links)
       
        print(f"Scraped page: {page_result}")
    return case_links

# Define range of pages to scrape
start_page = 1
end_page = 524 # Consider scraping this number instead using getNumberOfPages() or similar

isScraped = 1 # Flag to avoid scraping/downloading files again

if not isScraped:
    # Scrape pages and get all case links
    case_links = scrape_pages(start_page, end_page)

    # Download PDFs in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(download_pdf, case_links)


# List all PDF files in the current directory
files = [f for f in os.listdir() if f.endswith('.pdf')]

# Keywords to search for
#keywords = ["unlawfully killed", "stalker", "stalking", "partner", "relationship", "dom-5", "coercive", "assault", "marital", "section 20", "rape", "raped", "murdered", "domestic abuse"]
keywords = ["mental health", "psychosis", "depression", "anxiety", "delusion", "capacity", "psychiatry", "psychiatrist", "manic", "section"]
# Process PDFs in parallel and count keywords
word_count = []
with concurrent.futures.ThreadPoolExecutor() as executor:
    word_count = list(executor.map(lambda file: process_pdf(file, keywords), files))

# Convert word count to a DataFrame and save as CSV
word_count_df = pd.DataFrame(word_count, index=files, columns=keywords)
word_count_df = word_count_df[word_count_df.sum(axis=1) != 0] # Filter down rows where there are no keywords hit
print(word_count_df)

word_count_df.to_csv("wordcountTest.csv")