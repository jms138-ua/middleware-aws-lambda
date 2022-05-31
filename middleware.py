import json
import boto3

#POST: <url>/<tablename>            Insert item (item in request body)
#GET: <url>/<tablename>             List all items of table
#GET: <url>/<tablename>?ID=<id>     Get item by ID
#DELETE: <url>/<tablename>?ID=<id>  Delete item by ID

#Example: "Clientes", {"ID":0, "email":"test@email.com"}


class DBClient:
    def __init__(self):
        self.dynamodb_resource = boto3.resource("dynamodb")
        self.dynamodb_client = boto3.client("dynamodb")

    def list_table(self, tablename:str):
        return self.dynamodb_client.scan(TableName=tablename)

    def get(self, tablename:str, id:str):
        return self.dynamodb_client.get_item(TableName=tablename, Key={"ID":{"S":id}})

    def insert(self, tablename:str, args:dict):
        table = self.dynamodb_resource.Table(tablename)
        table.put_item(Item=args)

    def delete(self, tablename:str, id:str):
        table = self.dynamodb_resource.Table(tablename)
        table.delete_item(Key={"ID":id})


class API:
    NOT_IMPLEMENTED_MSG = {"Error":"HTTP Request not implemented"}

    def __init__(self):
        self.dbclient = DBClient()
        self.resp = {"Error":"Unknow"}

    def recv(self, event):
        httpmethod = event["httpMethod"]
        tablename = event["pathParameters"]["tabla"]

        if httpmethod == "POST":
            body = json.loads(event["body"])
            self.dbclient.insert(tablename, body)
            self.resp = {"POST":"Added"}

        elif httpmethod == "GET":
            args = event["queryStringParameters"]
            if args is None:
                self.resp = self.dbclient.list_table(tablename)
            elif "ID" in args:
                self.resp = self.dbclient.get(tablename, args["ID"])

        elif httpmethod == "DELETE":
            args = event["queryStringParameters"]
            if args is None:
                self.resp = API.NOT_IMPLEMENTED_MSG
            elif "ID" in args:
                self.resp = self.dbclient.delete(tablename, args["ID"])

        else:
            self.resp = API.NOT_IMPLEMENTED_MSG

    def send(self):
        if self.resp is None:
            return {"Error":"response is None"}

        #resp{key:{type:value},} -> resp{key:value,}
        def deserialize(resp, item):
            for key, value_dict in item.items():
                resp[key] = list(value_dict.values())[0]

        if "Items" in self.resp:
            for i, item in enumerate(self.resp["Items"]):
                deserialize(self.resp["Items"][i], item)
            return self.resp["Items"]

        if "Item" in self.resp:
            deserialize(self.resp["Item"], self.resp["Item"])
            return self.resp["Item"]

        return self.resp


def lambda_handler(event, context):
    api = API()
    api.recv(event)
    tosend = api.send()

    return {
        "statusCode":200,
        "body":json.dumps(tosend)
    }