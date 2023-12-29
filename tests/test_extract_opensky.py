import pytest
import requests
from datetime import datetime, timedelta
from context import extract


class TestOpenSkyAPICall:
    """Tests for OpenSky API extraction"""
    def test_range_above_api_limit(self):
        """Test if the function splits requested date range into multiple
        valid periods to API calls
        """
        start = datetime(2018, 1, 1)
        end = datetime(2018, 2, 1)      # Available date range in API
        test_period = extract.API_LIMIT_PERIOD + timedelta(days = 1)

        cnt_api_calls = 0
        for _ in extract.send_requests(
            "departure", 
            "EDDF", 
            start, 
            end, 
            test_period    # larger than API limit
        ):  # return a list for each successful API call
            cnt_api_calls += 1
        
        expected_api_calls = int((end - start) / extract.API_LIMIT_PERIOD) + 1
        assert cnt_api_calls == expected_api_calls