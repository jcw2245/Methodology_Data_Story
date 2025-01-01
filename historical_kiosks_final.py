import aiohttp
import asyncio
import pandas as pd
import csv

BASE_URL = "https://data.cityofnewyork.us/resource/quxm-hmyr.json"
LIMIT = 1000
OUTPUT_FILE = "nyc_data.csv"

async def get_total_count(session):
    """Fetch the total number of rows available in the dataset."""
    url = f"{BASE_URL}?$select=count(*)"
    for retry in range(3):  
        try:
            async with session.get(url, timeout=15) as response:
                if response.status == 200:
                    count_data = await response.json()
                    return int(count_data[0]['count'])
                elif response.status == 429:  # Too Many Requests
                    retry_after = int(response.headers.get("Retry-After", retry + 1))  # Use Retry-After if available
                    print(f"Rate limited while fetching total count. Retrying after {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                else:
                    print(f"Error fetching total count: {response.status}")
                    return 0
        except Exception as e:
            print(f"Error during count fetch attempt {retry + 1}: {e}")
            await asyncio.sleep(2 ** retry)  # Exponential backoff for retries
    print("Failed to fetch total count after multiple retries.")
    return 0

async def fetch_data(offset, session):
    """Fetch data for a specific offset."""
    url = f"{BASE_URL}?$limit={LIMIT}&$offset={offset}"
    print(f"Fetching data at offset {offset}")
    for retry in range(5):  # Retry up to 5 times
        try:
            async with session.get(url, timeout=15) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:  # Too Many Requests
                    retry_after = int(response.headers.get("Retry-After", retry + 1))  # Dynamically handle retry delay
                    print(f"Rate limited at offset {offset}, retrying after {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                else:
                    print(f"Error at offset {offset}: {response.status}")
                    return []
        except Exception as e:
            print(f"Retry {retry + 1} for offset {offset}: {e}")
            await asyncio.sleep(2 ** retry)  # Exponential backoff for retries
    print(f"Failed to fetch data at offset {offset} after multiple retries.")
    return []

async def process_and_save(offset, session):
    """Fetch data and append it to the CSV file."""
    data = await fetch_data(offset, session)
    if not data:
        print(f"No data returned at offset {offset}. Skipping.")
        return

    df = pd.DataFrame(data)
    df.to_csv(OUTPUT_FILE, mode='a', header=False, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"Processed {len(data)} rows at offset {offset}")

async def main():
    # Prepare the CSV file with a header
    initial_df = pd.DataFrame(columns=['column1', 'column2', 'column3'])  # Replace with actual column names
    initial_df.to_csv(OUTPUT_FILE, index=False)  # Write header

    async with aiohttp.ClientSession() as session:
        # Step 1: Try to fetch the total count
        total_rows = await get_total_count(session)
        if total_rows == 0:
            print("Failed to fetch total count. Using an estimated upper limit of 50000 rows.")
            total_rows = 50000  # Use an estimated value if total count fails

        print(f"Total rows to fetch: {total_rows}")
        
        # Step 2: Fetch data in batches with rate limiting
        batch_size = 5  # Number of concurrent tasks to run at a time
        for start_offset in range(0, total_rows, batch_size * LIMIT):
            tasks = [
                process_and_save(offset, session)
                for offset in range(start_offset, start_offset + (batch_size * LIMIT), LIMIT)
                if offset < total_rows
            ]
            await asyncio.gather(*tasks)
            await asyncio.sleep(5)  # Add a delay between batches to avoid rate-limiting

# Run the main function
asyncio.run(main())
import aiohttp
import asyncio
import pandas as pd

BASE_URL = "https://data.cityofnewyork.us/resource/quxm-hmyr.json"
LIMIT = 1000
OUTPUT_FILE = "nyc_data.csv"

async def fetch_data(offset, session):
    """Fetch data for a specific offset."""
    url = f"{BASE_URL}?$limit={LIMIT}&$offset={offset}"
    try:
        async with session.get(url, timeout=15) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"Error at offset {offset}: {response.status}")
                return []
    except Exception as e:
        print(f"Error at offset {offset}: {e}")
        return []

async def fetch_all_data(total_rows, session):
    """Fetch all data in batches."""
    tasks = [
        fetch_data(offset, session)
        for offset in range(0, total_rows, LIMIT)
    ]
    results = await asyncio.gather(*tasks)
    return [item for sublist in results for item in sublist]  # Flatten the results

async def main():
    async with aiohttp.ClientSession() as session:
        # Fetch total count (default to 50,000 if unknown)
        total_rows = 50000  # Simplified, assuming a large dataset

        # Fetch all data
        print(f"Fetching up to {total_rows} rows...")
        all_data = await fetch_all_data(total_rows, session)

        # Save to CSV
        if all_data:
            df = pd.DataFrame(all_data)
            df.to_csv(OUTPUT_FILE, index=False)
            print(f"Saved {len(df)} rows to {OUTPUT_FILE}")
        else:
            print("No data retrieved.")

# Run the script
asyncio.run(main())