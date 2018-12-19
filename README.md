# bitflyer executions history function for AWS Lambda(Python)



## How to develop

1. git clone https://github.com/codecomfort/bitflyer-data-getter.git
2. cd bitflyer-data-getter
3. pipenv install



## How to run(local)

1. Set your environment variables
    - export DISCORD_POST_URL="your discord webhooks address" (optional)
    - export S3_BUCKER_NAME="your S3 bucket name"

2. Edit sample data in lambda_function.py

    - event

3. pipenv shell

4. python lambda_function.py

5. then check your S3 bucket

    key name first(10 digits)-last(10 digits)

    ("first": 657569720, "last": 657569723 → 0657569720-0657569723)



