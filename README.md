# Scraper

## Setting up the environment

#### Virtualenv
I use virtualenv but it is not strictly speaking necessary

```
virtualenv venv
source /venv/bin/activate
```

#### Environment variables
You will an environment variable, ```SABETAY_DATABASE```

```
export SABETAY_DATABASE=postgresql://sabetay:sabetay123@localhost:5432/sabetay
export SABETAY_DATABASE=postgresql://user:password@address:port/
```
#### Python packages installation
```
pip3 install -r requirements.txt
or
pip install -r requirements.txt
```
#### Initializing the database
Create two files, ```zipcodes.txt``` and ```proxies.txt```

example zipcodes
```
//zipcodes.txt
90001
90002
```

example proxies (I use rotating proxies from webshare). 
NOTE: You will definetaly need proxies
```
p.webshare.io:19999
p.webshare.io:20000
```

Then, run this ONLY ONCE
```
cd sabetay/spiders
python ORM.py
```

This will create all tables and populate a table for zipcodes and for proxies.
You can also insert zipcodes and proxies through the psql shell

# Usage
```cd``` into the top level directory (the one that contains ```scrapy.cfg```)
```
scrapy crawl root
```

