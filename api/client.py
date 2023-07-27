import cherrypy # api server - creates an api
import requests, redis     
import pymongo  
import threading, time
import json
from config import MONGO_CLIENT, redis_host, redis_port, redis_password

class ApiRequest:

    def get_mongo(self):        # getting mongodb's connection
        return pymongo.MongoClient(MONGO_CLIENT)
    
    def get_redis(self):    #redis configuration and connection establsihment 
        return redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
    
    def get_endpoint(self, endpoint_id):    # find document query with required endpoint_id

        client = self.get_mongo()   # mongo client (cluster)
        db = client['GatewayX'] # in gatewayx db 
        collection = db['Headers']  # in headers collection
        query = {"endpoint_id": endpoint_id}     # document with required endpt_id
        res = collection.find_one(query)    # find query with required endpoint_id
        return res

    def auth_headers(self,endpoint_id, headers):    # header validation

        res = self.get_endpoint(endpoint_id)    # get the document
        auth_headers = res.get("auth")  # take value of object auth, consists api and session key
        for header in auth_headers:
            if header not in headers:   # if not present, false
                return False
            if headers[header] != auth_headers[header]:     # if doesn't match, false
                return False
            
        return True     # else true
    
    def get_resp(self, dest, event_handler, result_dict):   #get response
        result = requests.get(dest)     #api destination
        result_dict['data'] = result.json()
        event_handler.set() # set the event 

    def distributed_requests(self, dest1, dest2):   # Multithreaded Api Access
        event1 = threading.Event()  #threading events (set/not set)
        event2 = threading.Event()
        result1 = {}    # to get result data of requests
        result2 = {}
        res1 = threading.Thread(target=self.get_resp, args=(dest1, event1, result1))    #threading
        res2 = threading.Thread(target=self.get_resp, args=(dest2, event2, result2))
        res1.start()    #start the thread
        res2.start()

        while True:     # while loop needed to check in sleep time 1s continously whether events are set or not, if set, return results, else wait and check again
            if event2.is_set() and event1.is_set():     #it may take time to set the event, hence checks in loop
                return {"res1": result1['data'], "res2": result2['data']}
            time.sleep(1)

    @cherrypy.tools.json_out()  # 
    @cherrypy.tools.json_in()   #
    @cherrypy.expose    # makes handle_request function an endpoint
    def handle_request(self):

        headers = cherrypy.request.headers
        post_request = cherrypy.request.json
        endpoint_id = headers.get("Endpoint-Id")
        check_headers = self.auth_headers(endpoint_id, headers) #authenticate the headers
        if not check_headers:   # if not valid
            return {"status": "error", "message": "Invalid headers"}
        else:   #if valid
            details = self.get_endpoint(endpoint_id)    # get document
            req_type = details.get("type")  # configure type, dest and parallel
            req_dest = details.get("destination")
            parallel = details.get("parallel")  # multithreading yes/no
            if parallel:    # if multhtreading yes
                return self.distributed_requests(details.get("destination1"), details.get("destination2"))  # run simultaneously
            if req_type == 'GET':   # get req
                res = redis_connection.get('GET:' + req_dest)   # if available in cache
                if res: # if available in redis cache
                    logging.info("We got the result from cache !!! ")
                    return json.loads(res)  # return response 
                api_result = requests.get(url = req_dest).json()    # response from url
                redis_connection.setex('GET:' + req_dest, details['ttl'], json.dumps(api_result)) # set the resposne in cache
                return api_result
            elif req_type == 'POST':    # post req
                res = redis_connection.get('POST:' + req_dest + json.dumps(post_request))    
                if res: # if available in redis cache
                    logging.info("We got the result from cache !!! ")
                    return json.loads(res)  # return response
                api_result = requests.get(url = req_dest).json()
                redis_connection.setex('POST:' + req_dest + json.dumps(post_request), details['ttl'], json.dumps(api_result))  
                return requests.post(url = req_dest, json=post_request).json()  
            
api = ApiRequest()  # object of ApiRequest class
redis_connection = api.get_redis()  # establish connection with redis on the object
cherrypy.quickstart(api)    