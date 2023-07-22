import cherrypy # api server
import logging, requests, redis
import pymongo
import threading, time
import json
from config import MONGO_CLIENT, redis_host, redis_port, redis_password
logging.basicConfig(level=logging.INFO)

class ApiRequest:

    def get_mongo(self):
        return pymongo.MongoClient(MONGO_CLIENT)
    
    def get_redis(self):
        return redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
    
    def get_endpoint(self, endpoint_id):

        client = self.get_mongo()
        db = client['GatewayX']
        collection = db['Headers']
        query = {"endpoint_id": endpoint_id}
        res = collection.find_one(query)
        return res

    def auth_headers(self,endpoint_id, headers):

        res = self.get_endpoint(endpoint_id)
        auth_headers = res.get("auth")
        for header in auth_headers:
            if header not in headers:
                return False
            if headers[header] != auth_headers[header]:
                return False
            
        return True
    
    def get_resp(self, dest, event_handler, result_dict):
        result = requests.get(dest)
        result_dict['data'] = result.json()
        event_handler.set()

    def distributed_requests(self, dest1, dest2):
        event1 = threading.Event()
        event2 = threading.Event()
        result1 = {}
        result2 = {}
        res1 = threading.Thread(target=self.get_resp, args=(dest1, event1, result1))
        res2 = threading.Thread(target=self.get_resp, args=(dest2, event2, result2))
        res1.start()
        res2.start()

        while True:
            print(event1.is_set(), event2.is_set())
            if event2.is_set() and event1.is_set():
                return {"res1": result1['data'], "res2": result2['data']}
            time.sleep(1)

    @cherrypy.tools.json_out()
    @cherrypy.tools.json_in()
    @cherrypy.expose
    def handle_request(self):

        headers = cherrypy.request.headers
        post_request = cherrypy.request.json
        endpoint_id = headers.get("Endpoint-Id")
        check_headers = self.auth_headers(endpoint_id, headers)
        if not check_headers:
            return {"status": "error", "message": "Invalid headers"}
        else:
            details = self.get_endpoint(endpoint_id)
            req_type = details.get("type")
            req_dest = details.get("destination")
            parallel = details.get("parallel")
            if parallel:
                return self.distributed_requests(details.get("destination1"), details.get("destination2"))
            if req_type == 'GET':
                res = redis_connection.get('GET:' + req_dest)
                if res:
                    logging.info("We got the result from cache !!! ")
                    return json.loads(res)
                api_result = requests.get(url = req_dest).json()
                redis_connection.setex('GET:' + req_dest, details['cache'], json.dumps(api_result))
                return api_result
            elif req_type == 'POST':
                res = redis_connection.get('GET:' + req_dest + json.dumps(post_request))
                if res:
                    logging.info("We got the result from cache !!! ")
                    return json.loads(res)
                api_result = requests.get(url = req_dest).json()
                redis_connection.setex('GET:' + req_dest + json.dumps(post_request), details['cache'], json.dumps(api_result))
                return requests.post(url = req_dest, json=post_request).json()
            
api = ApiRequest()
redis_connection = api.get_redis()
cherrypy.quickstart(api)