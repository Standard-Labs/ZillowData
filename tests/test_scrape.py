import scraper.scrape as scrape
import pytest

@pytest.mark.skip()
def test_max_pages():
    
    assert scrape.get_max_pages("hicksville", "ny", "listing-agent") == 25