
# activating system
python3 manage.py --command run
if you want to put this task on background: 
nohup python3 manage.py --command run > nohup.log &


# test or run some functions in isolated mode
get offers from endpoint:
python3 manage.py --command get-offers --zip-code {zip-code}

process on zip_codes in database:
python3 manage.py --command process-zip-codes

process on offers by zip_code
python3 manage.py --command process-offers --zip-code {zip-code}


# activating FastAPI application in product mode
uvicorn api:app --reload
if you want to put this task on background: 
nohup uvicorn api:app --reload > api.log &