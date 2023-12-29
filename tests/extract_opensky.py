import pytest
import requests
from datetime import datetime, timedelta
from context import extract


class TestOpenSkyAPICall:
    """Test 
    """
    def test_range_above_api_limit(self):
        """Test if the function splits requested date range into multiple
        valid periods to API calls
        """
        start = datetime(2018, 1, 1)
        end = datetime(2018, 2, 1)      # Available date range in API
        test_period = extract.API_LIMIT_PERIOD + timedelta(days = 1)

        cnt_api_calls = 0
        for _ in extract.extract_flights_by_airport(
            "departure", 
            "EDDF", 
            start, 
            end, 
            test_period    # larger than API limit
        ):  # return a list for each successful API call
            cnt_api_calls += 1
        
        expected_api_calls = int((end - start) / extract.API_LIMIT_PERIOD) + 1
        assert cnt_api_calls == expected_api_calls

    def test_invalid_date(self):
        """Test if the function errors out when input dates 
        that are unavailable in the server database
        """
        valid_start = datetime(2018, 1, 1)   # Existing data point in server db
        valid_end = datetime(2018, 1, 2)   # Existing data point in server db
        invalid_start = datetime(500, 1, 1) # timestamp -46388678400
        invalid_end = datetime.now() + timedelta(weeks = 50)    

        # Functions returning generators must be iterated to be executed
        # First case
        with pytest.raises(ValueError):
            for _ in extract.extract_flights_by_airport(
                "departure",
                "EDDF",
                invalid_start,
                valid_end
            ):  
                break

        # Second case    
        with pytest.raises(ValueError):
            for _ in extract.extract_flights_by_airport(
                "departure",
                "EDDF",
                valid_start,
                invalid_end
            ):
                break

        # Final case    
        with pytest.raises(ValueError):
            for _ in extract.extract_flights_by_airport(
                "departure",
                "EDDF",
                invalid_start,
                invalid_end
            ):
                break