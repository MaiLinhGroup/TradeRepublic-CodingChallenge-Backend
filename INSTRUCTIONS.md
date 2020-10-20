# Trade Republic Coding Challenge
Thank you for your interest in Trade Republic.
To proceed with your interview process, we have prepared a short coding exercise for you.

The goal of this test is to see how you work in general and that tools you are comfortable with.

As this is a coding test and not a Math exam, algorithms/complex datastructures can be implemented the naive way and we can talk about possible performance improvements later.

## Quote history aggregator (Quotes)
Your task is to build the Quotes system that consumes Websocket Streams of our Partners and stores the data in an aggregated form for retrieval via a JSON-REST endpoint and a stream.

Our partners provide two websocket streams (ws://localhost:8080) plus a Website Preview of how the stream look (http://localhost:8080)
 * `/instruments` provides the currently available instruments with their [ISIN](https://en.wikipedia.org/wiki/International_Securities_Identification_Number) and a Description
   * when connecting to the stream, it gives all currently available Instruments
   * once connected it streams the addition/removal of instruments
     * Our partners assured us that ISINS are unique, but can in rare cases be reused once no Instrument with that ISIN is available anymore (has been deleted, etc.)
 * `/quotes` provides the most current price for an instrument every few seconds per available instrument



### Instrument-Price retrieval
The Users want to scroll through the available instruments with their current prices in the app, therefore
the system offers a JSON REST-API that allows a User (via App) to retrieve all currently available Instruments with the latest price.
 * When calling the Endpoint the System only returns Instruments that are currently available and have not been deleted
 * If no price has been received yet, the returned price for the respective Instrument is `NULL`

### Aggregated-Price History retrieval
Once the user selected a certain instrument, they want to see the Price history of the last 30 minutes to display a nice [Candlestick-chart](https://en.wikipedia.org/wiki/Candlestick_chart).
To display this Chart, the system needs to return the Candlesticks for the last 30 minutes.

The quotes System uses simplified 1 Minute Candlesticks
Every minute the previous candle is closed and a new candle is opened.
A candle considers all prices with a timstamp >= openTimestamp (inclusive) && <closingTimestamp(exclusive)

A candlestick has at least the following attributes
 *  Open timestamp
    * Timestamp when this candle was opened
 *  Open Price
    * The first price that was part of this Candle
 *  High Price
    * The highest Price during the period of this Candle
 *  Low Price
    * The lowest Price during the period of this Candle
 *  Closing Price
    * The last price that was part of this candle
 *  Close timestamp
    * Timestamp when this candle was closed

Example:
Assume the following data was received via stream:
```
@2019-03-05 13:00:01 price: 11
@2019-03-05 13:00:13 price: 15
@2019-03-05 13:00:19 price: 10
@2019-03-05 13:00:32 price: 13
@2019-03-05 13:00:49 price: 12
@2019-03-05 13:00:57 price: 12
```
The resulting minute candle would have these attributes
```
openTimestamp: 2019-03-05 13:00:00
openPrice: 11
highPrice: 15
lowPrice: 10
closePrice: 12
closeTimestamp: 13:01:00
```

If there weren't any prices received for a specific candle, values from the previous candle are reused.

### Hot Instrument stream
Trade Republic wants to provide their users with a list of the most volatile/interesting Instruments.
For that the Quotes system provides a stream of Instruments with the change in %, that have changed more than +/-10% in price relative to the price 5 minutes ago
 * Only the quote that actually increased the change to over 10%  should trigger an event that is streamed. A second event should only be emitted if the %change fell below 10% then increases to over 10% again (flipFlop)


## Future Development Discussion
* How would you change the system to provide scaling capabilities to 50.000 (or more) available `instruments`, each streaming `quotes` between once per second and every few seconds?
* How could this system be build in a way that supports failover capabilities so that multiple instances of the system could run simultaneously?


## Notes
### PartnerService
This coding challenge includes a runnable JAR (the partnerService) that provides the above mentioned Websockets.
Just run the jar in a terminal with `-h` or `--help` and it will print how to use it

Once started, it provides a website under `localhost:8080` where you can get a preview of how the Stream works

If you restart the PartnerService, you will have to clean up any data you might have persisted, since it will generate new ISINS and does not retain state from any previous runs

### `/instrument` Specification
The `/instruments` Websocket provides all currently active Instruments `onConnect` as well as a stream of add/delete events of instruments.
When you receive a `delete` event, the instrument is no longer available and will not receive any more qoutes (beware out of order messages on the `/quotes` stream)
The Instruments are uniquely identified by their `isin`. Beware, ISINs can be reused AFTER an instrument has been deleted. In any case, you would see a regular `ADD` event for this new instrument, even when it reuses an ISIN.
```
{
    // The type of the event. ADD if an instrument is ADDED
    // DELETE if an instrument is deleted
    "type": "DELETE"
    {
        //The Payload
        "data": {
            //The Description of the instrument
            "description": "elementum eos accumsan orci constituto antiopam",
            //The ISIN of this instrument
            "isin": "LS342I184454"
        }
    }
}
```


### `/quotes` Specification
The `/quotes` Websocket provides prices for available instruments at an arbitrary rate.
It only streams prices for available instruments (beware out of order messages)
```
{
    // The type of the event.
    // QUOTE if an new price is available for an instrument identified by the ISIN
    "type": "QUOTE"
    {
        //The Payload
        "data": {
            //The price of the instrument with arbitray precision
            "price": 1365.25,
            //The ISIN of this instrument
            "isin": "LS342I184454"
        }
    }
}
```


## Hints
### Software/Frameworks/Libs used at Trade Republic
Here is a short list of Software/Libs that we are using. This does in NO WAY mean that you should also use the same Software.
* Kotlin/Java
* Spring
* VertX
* Jooq
* Hibernate
* Gradle/Maven
* FlyWay
* Docker/Docker-Compose

## Handing in the Results
Please hand in your Result as a zip file to codingchallenge@traderepublic.com




In case you have any questions, feel free to reach out to us at codingchallenge@traderepublic.com