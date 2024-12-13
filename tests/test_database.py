import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession
from database.async_inserter import AsyncInserter
from scraper.models import Agent


# Test general insertion, retrieval through endpoints, and deletion of mainly the listings as the cascade delete will take care of the rest
@pytest.mark.asyncio
async def test_insert_and_query_agent(db_session: AsyncSession, client: TestClient, async_inserter: AsyncInserter, get_agent_model:Agent, get_agent_model2: Agent):

    # to ensure that we have a clean slate in case something goes wrong prior 
    delete_response = client.delete(f"/agent/{get_agent_model.encodedzuid}")
    delete_response = client.delete(f"/agent/{get_agent_model2.encodedzuid}")

    await async_inserter.insert_agents([get_agent_model], "TEST CITY", "TEST STATE")

    response = client.get(f"/agent/{get_agent_model.encodedzuid}")
    assert response.status_code == 200
    assert response.json()["encodedzuid"] == get_agent_model.encodedzuid
    
    response = client.get(f"/agentIDs/by_city_state/TEST CITY/TEST STATE")
    assert response.status_code == 200
    assert get_agent_model.encodedzuid in response.json()

    response = client.get(f"/agent/{get_agent_model.encodedzuid}/cities")
    assert response.status_code == 200
    assert response.json()[0][0] == "TEST CITY"

    response = client.get(f"/agent/{get_agent_model.encodedzuid}/phones")
    assert response.status_code == 200
    phones = response.json()
    expected_phones = [
        {"phone": "123-456-7890", "type": "primary"},
        {"phone": "098-765-4321", "type": "brokerage"},
        {"phone": "111-222-3333", "type": "business"}
    ]
    assert phones == expected_phones

    response = client.get(f"/agent/{get_agent_model.encodedzuid}/websites")
    assert response.status_code == 200
    websites = response.json()
    website_urls = [website["website_url"] for website in websites]
    expected_urls = ["http://website1.com/", "http://website2.com/"]
    assert all(url in website_urls for url in expected_urls)

    
    response = client.get(f"/agent/{get_agent_model.encodedzuid}/listings")
    assert response.status_code == 200
    listings = response.json()
    expected_listings = [1001, 1002]
    assert all(listing in expected_listings for listing in listings)


    response = client.get(f"/listing/1001")
    assert response.status_code == 200
    assert response.json()["zpid"] == 1001

    response = client.get(f"/listing/1002")
    assert response.status_code == 200
    assert response.json()["zpid"] == 1002


    delete_response = client.delete(f"/agent/{get_agent_model.encodedzuid}")
    assert delete_response.status_code == 200

    attempt_query_response = client.get(f"/agent/{get_agent_model.encodedzuid}")
    assert attempt_query_response.status_code == 404

    check_listing = client.get(f"/listing/1001")
    assert check_listing.status_code == 404

    check_listing = client.get(f"/listing/1002")
    assert check_listing.status_code == 404


# Test to ensure that a listing with multiple agents persists if one of the agents is deleted, and then is completly deleted if all linked agents are deleted
@pytest.mark.asyncio
async def test_insert_and_query_agent_multiple_agents(db_session: AsyncSession, client: TestClient, async_inserter: AsyncInserter, get_agent_model:Agent, get_agent_model2: Agent):

    # to ensure that we have a clean slate in case something goes wrong prior 
    delete_response = client.delete(f"/agent/{get_agent_model.encodedzuid}")
    delete_response = client.delete(f"/agent/{get_agent_model2.encodedzuid}")
    
    
    get_agent_model.pastSales = get_agent_model2.pastSales
    await async_inserter.insert_agents([get_agent_model, get_agent_model2], "TEST CITY", "TEST STATE")

    res = client.get(f"/agent/{get_agent_model.encodedzuid}/listings")
    assert res.status_code == 200
    listings = res.json()
    assert 2001 in listings
    assert 2002 in listings


    res = client.get(f"/agent/{get_agent_model2.encodedzuid}/listings")
    assert res.status_code == 200
    listings = res.json()
    assert 2001 in listings
    assert 2002 in listings


    delete_response = client.delete(f"/agent/{get_agent_model.encodedzuid}")
    assert delete_response.status_code == 200

    # both listings should still exist as the other agent is still linked
    check_listing = client.get(f"/listing/2001")
    assert check_listing.status_code == 200
    check_listing = client.get(f"/listing/2002")
    assert check_listing.status_code == 200

    delete_response = client.delete(f"/agent/{get_agent_model2.encodedzuid}")
    assert delete_response.status_code == 200

    # now both listings should be deleted
    check_listing = client.get(f"/listing/2001")
    assert check_listing.status_code == 404
    check_listing = client.get(f"/listing/2002")
    assert check_listing.status_code == 404


# Test to ensure that listings are updated/old listings are deleted if /update/listings endpoint is hit
@pytest.mark.asyncio
async def test_insert_and_query_listings(db_session: AsyncSession, client: TestClient, async_inserter: AsyncInserter, get_agent_model:Agent, get_agent_model2: Agent):

    # to ensure that we have a clean slate in case something goes wrong prior 
    delete_response = client.delete(f"/agent/{get_agent_model.encodedzuid}")
    delete_response = client.delete(f"/agent/{get_agent_model2.encodedzuid}")


    await async_inserter.insert_agents([get_agent_model], "TEST CITY", "TEST STATE")

    response = client.get(f"/agent/{get_agent_model.encodedzuid}/listings")
    assert response.status_code == 200
    listings = response.json()
    assert 1001 in listings
    assert 1002 in listings

    # update the listings
    get_agent_model.pastSales[0].zpid = 9999
    get_agent_model.pastSales[0].address.line1 = "9999 MAIN STREET"
    await async_inserter.insert_updated_listings([get_agent_model], "TEST CITY", "TEST STATE")

    response = client.get(f"/agent/{get_agent_model.encodedzuid}/listings")
    assert response.status_code == 200
    listings = response.json()
    assert 9999 in listings
    assert 1002 in listings
    assert 1001 not in listings # should be deleted from agent's listings

    # should not be present in the database at all either
    check_listing = client.get(f"/listing/1001")
    assert check_listing.status_code == 404

    # check if the address is updated
    response = client.get(f"/listing/9999")
    assert response.status_code == 200
    assert response.json()['line1'] == "9999 MAIN STREET"

    delete_response = client.delete(f"/agent/{get_agent_model.encodedzuid}")
    assert delete_response.status_code == 200


# Test to ensure that agent core data is updated if the update_existing flag is set to True, and not updated if it is set to False
@pytest.mark.asyncio
async def test_insert_and_query_agent_update_existing(db_session: AsyncSession, client: TestClient, async_inserter: AsyncInserter, get_agent_model:Agent, get_agent_model2: Agent):
    
        # to ensure that we have a clean slate in case something goes wrong prior 
        delete_response = client.delete(f"/agent/{get_agent_model.encodedzuid}")
        delete_response = client.delete(f"/agent/{get_agent_model2.encodedzuid}")
    
        await async_inserter.insert_agents([get_agent_model], "TEST CITY", "TEST STATE")
    
        response = client.get(f"/agent/{get_agent_model.encodedzuid}")
        assert response.status_code == 200
        assert response.json()["full_name"] == get_agent_model.full_name

        get_agent_model.full_name = "NEW NAME"
        # should NOT update the name
        await async_inserter.insert_agents([get_agent_model], "TEST CITY", "TEST STATE")
        response = client.get(f"/agent/{get_agent_model.encodedzuid}")
        assert response.status_code == 200
        assert response.json()["full_name"] != get_agent_model.full_name

        # SHOULD update the name
        await async_inserter.insert_agents([get_agent_model], "TEST CITY", "TEST STATE", update_existing=True)
        response = client.get(f"/agent/{get_agent_model.encodedzuid}")
        assert response.status_code == 200
        assert response.json()["full_name"] == get_agent_model.full_name
    
        delete_response = client.delete(f"/agent/{get_agent_model.encodedzuid}")
        assert delete_response.status_code == 200
    