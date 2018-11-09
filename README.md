# PDL Wrapper

This utility logs calls to PDL API into DynamoDB.

## Installation 

Add this line to your requirements.txt:
```
git+git://github.com/tfc-code/pdl-wrapper@master#egg=pdlwrapper
```

## Usage

```py
pdl_client = pdlwrapper.Client(
    account_id="bernie2020",
    env="prod", # can be dev, test or prod
    pdl_api_key="pdl_key",
    aws_access_key_id="aws_key",
    aws_secret_access_key="aws_secret",
    aws_region_name="us-east-2",
)

# See https://github.com/peopledatalabs/docs for valid attributes
payload = {
    "required": "emails AND phone_numbers",
    "country": "United States",
    "name": "Tom Steyer",
    "locality": "New York"
}

resp = pdl_client.get_person(params=payload)
```

The `resp` object is simply the return value of `requests.get()`.
