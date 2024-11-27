import scraper.scrape as scrape

def test_max_pages():
    
    assert scrape.get_max_pages("hicksville", "ny", "listing-agent") == 15