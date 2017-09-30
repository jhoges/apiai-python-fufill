# -*- coding:utf8 -*-
# !/usr/bin/env python
# Copyright 2017 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
from future.standard_library import install_aliases
install_aliases()

from urllib.parse import urlparse, urlencode
from urllib.request import urlopen, Request
from urllib.error import HTTPError

import json
import os

from flask import Flask
from flask import request
from flask import make_response

# Flask app should start in global layout
app = Flask(__name__)

db = MySQLdb.connect("myinstance","root","472union", "dfp", charset='utf8')

@app.route('/webhook', methods=['POST'])
def webhook():
    req = request.get_json(silent=True, force=True)

    print("Request:")
    print(json.dumps(req, indent=4))

    res = processRequest(req)

    res = json.dumps(res, indent=4)
    # print(res)
    r = make_response(res)
    r.headers['Content-Type'] = 'application/json'
    return r

#FROM API.AI
    # "result": {
    #     "source": "agent",
    #     "resolvedQuery": "how many impressions did gq serve yesterday?",
    #     "action": "capacity",
    #     "actionIncomplete": false,
    #     "parameters": {
    #       "brand": "gq",
    #       "date-time": "2017-09-15",
    #       "geo-country": "",
    #       "metric": "impressions"
    #     },
    #     "contexts": [
    #       {
    #         "name": "date-time",
    #         "parameters": {
    #           "date-time.original": "yesterday?",
    #           "geo-country.original": "",
    #           "metric": "impressions",
    #           "metric.original": "impressions",
    #           "date-time": "2017-09-15",
    #           "brand.original": "gq",
    #           "brand": "gq",
    #           "geo-country": ""
    #         },
    #         "lifespan": 5
    #       }

  # "result": {
  #   "source": "agent",
  #   "resolvedQuery": "what's the weather in miami",
  #   "action": "yahooWeatherForecast",
  #   "actionIncomplete": false,
  #   "parameters": {
  #     "geo-city": "Miami",
  #     "zip-code": ""
  #   }

def processRequest(req):

    if req.get("result").get("action") != "yahooWeatherForecast":
        return {}

#    baseurl = "https://query.yahooapis.com/v1/public/yql?"
    yql_query = makeSqlQuery(req)
    if yql_query is None:
        return {}
    
    
    cursor = db.cursor()
    result=cursor.execute(makeSqlQuery(req))
    #old code yql_url = baseurl + urlencode({'q': yql_query}) + "&format=json"
    #old code  result = urlopen(yql_url).read()
    result =cursor.execute('""'+makeSqlQuery(req)+ '""')
    data = json.loads(result)
    res = makeWebhookResult(data)
    return res


def makeSqlQuery(req):
    result = req.get("result")
    parameters = result.get("parameters")
        #change this
    brand = parameters.get("brand")
    aidate = parameters.get("date-time")
    country = parameters.get("geo-country")
    rawmetric = parameters.get("metric")

    options = {'impressions':"sum(imps)",'clicks': "count(1)",'sell-thru' :"case when line_item_type like "'S%'" then count(1) else 0 end / count(1)", 'click-thru':doThere, 'ecpm':doThere, 'rpm':doThere}
    metric = options.get(rawmetric)

    table_sourcedict = {"impressions":"hive.dfp.network_impressions","clicks":"hive.dfp.network_clicks"}
    table_source = options.get(table_sourcedict)

        #log count
    return "select " + metric + " from " table_source " where dt >= "+ aidate +" and brand = " + brand + ';'
        # select sum(imps) from dfp.network_impressions where dt >= "2017-09-15" and brand = "GQ";
        #ratio return "select " + numerator +"/" + denomenator + " from " table_ratio " where dt >= "+ aidate +" and brand = " + brand
    

def makeWebhookResult(data):
    query = data.get('query')
    if query is None:
        return {}

    result = query.get('results')
    if result is None:
        return {}

    channel = result.get('channel')
    if channel is None:
        return {}

    item = channel.get('item')
    location = channel.get('location')
    units = channel.get('units')
    if (location is None) or (item is None) or (units is None):
        return {}

    condition = item.get('condition')
    if condition is None:
        return {}


        
    speech = "Today in " + location.get('city') + ": " + condition.get('text') + \
             ", the temperature is " + condition.get('temp') + " " + units.get('temperature')

    print("Response:")
    print(speech)

    return {
        "speech": speech,
        "displayText": speech,
        # "data": data,
        # "contextOut": [],
        "source": "apiai-weather-webhook-sample"
    }


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))

    print("Starting app on port %d" % port)

    app.run(debug=False, port=port, host='0.0.0.0')