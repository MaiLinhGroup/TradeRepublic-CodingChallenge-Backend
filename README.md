[Trade Republic](https://traderepublic.com/de-de) Backend Coding Challenge
=====

## Prerequisites

- the runnable JAR included in the coding challenge providing the Websockets is stored on Google Drive and can to be downloaded from [here](https://drive.google.com/file/d/1mdyg3ru0ybqIwNhw16BiKPriWjeYfrKz/view?usp=sharing)
- run the PartnerService websocket server with `java -jar partner-service-1.0-all.jar` to consume the endpoints in the application `ws://localhost:8080/instruments`, `ws://localhost:8080/quotes` and  `http://localhost:8080`
- the python library [asyncio](https://docs.python.org/3/library/asyncio.html#module-asyncio) which introduces `async/await` to python is only compatible with Python 3
- the dependencies of this project is targeting a `python_version = "3.8"`, if you don't want to mess up with your current system python or want to keep your current python, follow [this post](https://hackernoon.com/reaching-python-development-nirvana-bb5692adf30c) to install pyenv to manage multiple python installations
- install pipenv the dependency manager with `python3 -m pip install pipenv` or `brew install pipenv`
- finally install all dependencies with `pipenv install`

## Run the application

- run the main application with `pipenv run python app.py`
- once the application is started, it will serves a webserver on `http://http://localhost:8081` with the following two JSON-REST API endpoints:
    - GET `http://localhost:8081/prices`: this will response with a JSON file containing all available (not deleted) instruments with their recent prices
    - GET `http://localhost:8081/prices/{isin}`: this will take the `ISIN` number of an instrument and return a JSON file with the price history of that instrument for last 30 minutes, the price history itself is list of objects with fields that can be used to create a __Candlestick-chart__

## Notes:

- The application is missing the websocket endpoint to accomplish the requested feature [Hot Instrument stream](INSTRUCTIONS.md###Hot-Instrument-stream)
