import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import networkx as nx
import time
import os
import pickle
from collections import deque
from datetime import datetime

# Parameters
domain = "visakanv.com"
base_url = f"https://{domain}/"
timeout = 5
checkpoint_interval = 50
checkpoint_folder = "checkpoints"
output_file = "visakanvdotcom_graph.graphml"
max_pages = 10000
request_delay = 0.1
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36"
}

# Ensure checkpoint folder exists
os.makedirs(checkpoint_folder, exist_ok=True)
checkpoint_file = os.path.join(checkpoint_folder, "scraper_checkpoint.pkl")

# Initialize structures
graph = nx.DiGraph()
visited = set()
queued = set()  # Track URLs already in queue

def is_valid_url(url):
    """Check if URL is valid and belongs to our domain."""
    try:
        parsed = urlparse(url)
        return all([
            parsed.scheme in ['http', 'https'],
            parsed.netloc.endswith(domain),
            not any(ext in parsed.path for ext in ['.jpg', '.jpeg', '.png', '.gif', '.css', '.js']),
            '#' not in url  # Avoid anchor links to same page
        ])
    except:
        return False

def fetch_page_metadata(url, soup):
    """Extract metadata from a page."""
    metadata = {
        'url': url,
        'title': soup.title.string.strip() if soup.title else 'No Title',
        'word_count': len(soup.get_text().split())
    }
    
    # Try to find publication date
    # First try meta tag
    date_meta = soup.find('meta', property='article:published_time')
    if date_meta:
        metadata['pub_date'] = date_meta.get('content', 'Unknown')
    else:
        # Try to find date in URL
        try:
            url_parts = url.split('/')
            for i, part in enumerate(url_parts):
                if part.isdigit() and len(part) == 4:  # Possible year
                    if (i+1 < len(url_parts) and url_parts[i+1].isdigit() and 
                        1 <= int(url_parts[i+1]) <= 12):  # Month
                        metadata['pub_date'] = f"{part}-{url_parts[i+1]}"
        except:
            metadata['pub_date'] = 'Unknown'
    
    return metadata

def fetch_links(url):
    """Fetch links and metadata from a page."""
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Get metadata first
        metadata = fetch_page_metadata(url, soup)
        
        # Then get links
        links = set()
        for a_tag in soup.find_all("a", href=True):
            try:
                href = a_tag["href"]
                full_url = urljoin(url, href)
                parsed = urlparse(full_url)
                clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                if parsed.query:
                    clean_url += f"?{parsed.query}"
                
                if is_valid_url(clean_url):
                    links.add(clean_url)
                    
            except Exception as e:
                print(f"Error processing link in {url}: {e}")
                continue
                
        return links, metadata
        
    except requests.RequestException as e:
        print(f"Failed to fetch {url}: {e}")
        return set(), {}
    except Exception as e:
        print(f"Unexpected error processing {url}: {e}")
        return set(), {}
def analyze_queue():
    """Analyze current queue contents and print insights."""
    if not queue:
        return
    
    # Convert queue to list for analysis without disturbing it
    queue_list = list(queue)
    
    # Get endpoint patterns
    endpoints = {}
    for url in queue_list:
        # Get the path part of the URL
        path = urlparse(url).path
        parts = path.strip('/').split('/')
        
        # Get the first part of the path as the endpoint
        endpoint = parts[0] if parts else 'root'
        endpoints[endpoint] = endpoints.get(endpoint, 0) + 1
    
    print("\nQueue Analysis:")
    print(f"Total URLs in queue: {len(queue_list)}")
    print("\nTop endpoints:")
    for endpoint, count in sorted(endpoints.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"/{endpoint}: {count} URLs")
    
    # Sample some URLs from the queue
    print("\nSample URLs from queue:")
    sample_size = min(5, len(queue_list))
    for url in queue_list[:sample_size]:
        print(url)

def save_checkpoint():
    with open(checkpoint_file, "wb") as f:
        pickle.dump((graph, visited, queue, queued), f)
    print(f"\nCheckpoint saved. Visited: {len(visited)} pages.")
    analyze_queue()

def scrape():
    global queue, queued
    counter = len(visited)
    
    while queue and counter < max_pages:
        counter += 1  # Increment first
        
        url = queue.popleft()
        queued.remove(url)
        
        if url in visited:
            counter -= 1  # Decrement if we skip this URL
            continue
            
        links, metadata = fetch_links(url)
        time.sleep(request_delay)

        progress_msg = (
            f"\n[{counter}/{max_pages}] Processing:"
            f"\nURL: {url}"
            f"\nTitle: {metadata.get('title', 'No title')}"
            f"\nWords: {metadata.get('word_count', 0)}"
            f"\nPub Date: {metadata.get('pub_date', 'Unknown')}"
            f"\nQueue size: {len(queue)}"
            f"\n{'-' * 80}"
        )
        print(progress_msg)
        
        visited.add(url)
        graph.add_node(url, **metadata)
        
        for link in links:
            graph.add_edge(url, link)
            if link not in visited and link not in queued:
                queue.append(link)
                queued.add(link)
                
        if counter % checkpoint_interval == 0:
            save_checkpoint()
            
    return counter

if __name__ == "__main__":
    try:
        # Load checkpoint if it exists
        # In main:
        if os.path.exists(checkpoint_file):
            print("Loading checkpoint...")
            with open(checkpoint_file, "rb") as f:
                graph, visited, queue, queued = pickle.load(f)
            print(f"Checkpoint loaded. Resuming from {len(visited)} visited pages. Queue size: {len(queue)}")
        else:
            print("Starting new scrape...")
            queue = deque([base_url])
            queued = {base_url}

        # Start/resume scraping
        total_pages = scrape()
        
        # Export graph
        print(f"Scraping completed. Visited {total_pages} pages.")
        nx.write_graphml(graph, output_file)
        print(f"Graph exported to {output_file}")
            
    except KeyboardInterrupt:
        print("\nScraping interrupted. Saving checkpoint...")
        save_checkpoint()
        print("Checkpoint saved. You can resume later.")
        
    except Exception as e:
        print(f"Error occurred: {e}")
        print("Saving checkpoint before exit...")
        save_checkpoint()
        raise