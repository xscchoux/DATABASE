from flask import Flask
from aeneid.dbservices import dataservice as ds
from flask import Flask
from flask import request
import os
import json
import copy
from aeneid.utils import utils as utils
import re
from aeneid.utils import webutils as wu
from aeneid.dbservices.DataExceptions import DataException
from flask import Response
from urllib.parse import urlencode
import logging

# Default delimiter to delineate primary key fields in string.
key_delimiter = "_"
predefined_q_params  = ['fields', 'limit', 'offset','order_by']
app = Flask(__name__)

def compute_links(result, limit, offset):
    result['links'] = []
    # print("request url = ",request.url)
    self = {"rel": "current", "href": request.url}
    result['links'].append(self)

    if (offset is not None) and (limit is not None):
        next_offset = int(offset) + int(limit)

        base = request.base_url
        args = {}
        for k, v in request.args.items():
            if not k =='offset':
                args[k] = v
            else:
                args[k] = next_offset


        args['offset'] = next_offset
        args['limit'] = limit
        params = urlencode(args)

        next_r = base + "?" + params
        next_l = {'rel': 'next', 'href': next_r}
        result['links'].append(next_l)

    return result

@app.route('/')
def hello_world():
    return  """
            You probably want to go either to the content home page or call an API at /api.
            When you despair during completing the homework, remember that
            Audentes fortuna iuvat.
            """

@app.route('/explain', methods=['GET', 'PUT', 'POST', 'DELETE'])
def explain_what():

    result = "Explain what?"
    response = Response(result, status=200, mimetype="text/plain")

    return response

@app.route('/explain/<concept>', methods=['GET', 'PUT', 'POST', 'DELETE'])
def explain(concept):

    mt = "text/plain"

    if concept == "route":
        result = """
                    A route definition has the form /x/y/z.
                    If an element in the definition is of the for <x>,
                    Flask passes the element's value to a parameter x in the function definition.
                    """
    elif concept == 'request':
        result = """
                http://flask.pocoo.org/docs/1.0/api/#incoming-request-data
                explains the request object.
            """
    elif concept == 'method':
        method = request.method

        result = """
                    The @app.route() example shows how to define eligible methods.
                    explains the request object. The Flask framework request.method
                    is how you determine the method sent.
                    
                    This request sent the method:
                    """ \
                    + request.method
    elif concept == 'query':
        result = """
                    A query string is of the form '?param1=value1&param2=value2.'
                    Try invoking ' http://127.0.0.1:5000/explain/query?param1=value1&param2=value2.
                    
                """

        if len(request.args) > 0:
            result += """
                Query parameters are:
                """
            qparams = str(request.args)
            result += qparams
    elif concept == "body":
        if request.method != 'PUT' and request.method != 'POST':
            result = """
                Only PUT and GET have bodies/data.
            """
        else:
            result = """
                The content type was
            """ + request.content_type

            if "text/plain" in request.content_type:
                result += """
                You sent plain text.
                
                request.data will contain the body.
                
                Your plain text was:
                
                """ + str(request.data) + \
                """
                
                Do not worry about the b'' thing. That is Python showing the string encoding.
                """
            elif "application/json" in request.content_type:
                js = request.get_json()
                mt = "application/json"
                result = {
                    "YouSent": "Some JSON. Cool!",
                    "Note": "The cool kids use JSON.",
                    "YourJSONWas": js
                }
                result = json.dumps(result, indent=2)
            else:
                """
                I have no idea what you sent.
                """
    else:
        result = """
            I should not have to explain all of these concepts. You should be able to read the documents.
        """

    response = Response(result, status=200, mimetype=mt)

    return response

@app.route('/api')
def api():
    return 'You probably want to call an API on one of the resources.'

def get_location(dbname, resource_name, k):

    ks = [str(kk) for kk in k.values()]
    ks = "_".join(ks)
    result = "/api/" + dbname + "/" + resource_name + "/" + ks
    return result

def get_context():
    result = {}
    # Look for the fields=f1, f2, ... argument in the query parameters.
    field_list = request.args.get('fields',None)
    if field_list is not None:
        field_list = field_list.split(",")
        result['fields'] = field_list
    q_params = {}
    for k,v in request.args.items():
        # print("k v",k,v)
        if not k in predefined_q_params:
            q_params[k] = v
    result['query_params'] = q_params
    print("query_params = ",q_params)
    limit = request.args.get("limit",None)
    result['limit'] = limit
    offset = request.args.get("offset",None)
    result['offset'] = offset
    order_by = request.args.get("order_by",None)
    result['order_by'] = order_by
    children = request.args.get("children",None)
    result['children'] = children
    logging.debug("request context = " + utils.safe_dumps(result))
    print("get_context result = ",result)
    return result

def process_all_links(dbname, resource_name, result, context):
    return None

@app.route('/api/<dbname>/<resource_name>q=<some_query_string>', methods=['GET'])
def handle_path(dbname, resource_name, context):
    resp = Response("Internal server error", status = 500, mimetype="text/plain")
    context = get_context()

    try:
        limit = context.get('limit', 10)  # don't assign
        offset = context.get('offset', 0)
        order_by = context.get('order_by',None)
        resource = dbname + "." + resource_name

        if (limit is not None) or (offset is not None) or (order_by is not None) or (request.method != 'GET'):
            msg = "limit, offset and order_by not suppported by handle_path"
            resp = Response(msg, status=418, mimetype = "text/plain")
        else:
            tmp = context.get("query_params",None)
            children = context.get("children")
            field_list = context.get("fields", None)

            result = ds.get_by_query_from_h(resource_name, children, tmp, field_list)
        if result:
            result_data = json.dumps(result, default=str)
            resp = Response(result_data, status=200, mimetype='application/json')
        else:
            resp = Response("Not found", status = 404, mimetype="text/plain")
    except Exception as e:
        # We need a better overall approach to generating correct errors.
        utils.debug_message("Something awlful happened, e = ", e)
        print(e)
    return resp

@app.route('/api/<dbname>/<resource_name>/<primary_key>/<sub_resource>', methods=['GET','POST'])
def handle_path_resource(dbname, resource_name, primary_key, sub_resource):
    resp = Response("Internal server error", status = 500, mimetype='text/plain')
    print("handle path resource")
    context = get_context()

    limit = context.get('limit', None)
    offset = context.get('offset', None)
    order_by = context.get('order_by', None)

    if limit is None:
        limit = 10
        context['limit'] = 10
    if offset is None:
        offset = 0
        context['offset'] = 0

    try:
        result = None

        key_columns = primary_key.split(key_delimiter)
        print("key_columns",key_columns)
        resource = dbname + "." + resource_name
        subresource = dbname + "." + sub_resource
        print("subresource",subresource)

        if request.method == 'GET':
            field_list = context.get('fields', None)
            query_param = context.get('query_params',None)
            print("field_list = ",field_list)
            print("query_params= ",query_param)
            result = ds.get_by_primary_key_path(resource, query_param,key_columns, sub_resource, field_list = field_list, limit=limit, offset=offset, order_by=order_by)
            print("result in handle path source",result)
            if result:
                # We managed to find a row. Return JSON data and 200
                result = {resource: result}
                result = compute_links(result, limit, offset)
                result_data = json.dumps(result, default=str)
                resp = Response(result_data, status=200, mimetype='application/json')
            else:
                # We did not get an exception and we did not get data, therefore this is 404 not found.
                resp = Response("Not found", status=404, mimetype="text/plain")

        elif request.method == 'POST':

            new_r = request.json
            print("in POST, ",resource, key_columns, sub_resource, new_r)
            result = ds.insert_by_path(resource, key_columns, subresource, new_r)  # lahman2017.people ['willite01'] batting {'H': '100', 'HR': '50', 'AB': '100', 'yearID': '2', 'teamID': 'BOS', 'stint': '1'}
            print("insert_by_path succeed!")
            if result is not None:
                location = get_location(dbname, sub_resource, result)
                resp = Response('created', status = 201, mimetype='text/plain')
                resp.headers['Location'] = location
    except Exception as e:
        # We need a better overall approach to generating correct errors.
        utils.debug_message("Something awlful happened, e = ", e)
    return resp


@app.route('/api/<dbname>/<resource_name>/<primary_key>', methods=['GET', 'PUT', 'DELETE'])
def handle_resource(dbname, resource_name, primary_key):

    resp = Response("Internal server error", status=500, mimetype="text/plain")
    limit = request.args.get('limit', 10)
    offset = request.args.get('offset', 0)
    order_by = request.args.get('order_by', None)
    try:
        print("handle resource")
        # The design pattern is that the primary key comes in in the form value1_value2_value3
        key_columns = primary_key.split(key_delimiter)

        # Merge dbname and resource names to form the dbschema.tablename element for the resource.
        # This should probably occur in the data service and not here.
        resource = dbname + "." + resource_name

        if request.method == 'GET':
            # Look for the fields=f1,f2, ... argument in the query parameters.
            field_list = request.args.get('fields', None)
            if field_list is not None:
                field_list = field_list.split(",")

            # Call the data service layer.
            print(resource, key_columns,field_list)
            result = ds.get_by_primary_key(resource, key_columns, field_list=field_list)

            if result:
                # We managed to find a row. Return JSON data and 200
                result = {resource : result}
                result = compute_links(result, limit, offset)
                result_data = json.dumps(result, default=str)
                resp = Response(result_data, status=200, mimetype='application/json')
            else:
                resp = Response("NOT FOUND", status=404, mimetype="text/plain")

        elif request.method == "DELETE":
            result = ds.delete(resource, key_columns)
            if result and result >= 1:
                resp = Response("OK.", status=200, mimetype="application/json")
            else:
                resp = Response("NOT FOUND", status=404, mimetype="text/plain")

        elif request.method =='PUT':
            new_v = request.get_json()
            print("PUT, request we get : ", new_v)
            result = ds.update_by_key(resource, key_columns, new_v)
            if result == 1:
                resp = Response('OK', status=200, mimetype = 'text/plain')
            else:
                resp = Response('NOT FOUND', status=404, mimetype='text/plain')

        else:
            resp = Response("Should be GET, PUT or DELETE", status=422, mimetype="text/plain")

    except Exception as e:
        # We need a better overall approach to generating correct errors.
        utils.debug_message("Something awlful happened, e = ", e)

    return resp

@app.route('/api/<dbname>/<resource_name>', methods=['GET', 'POST'])
def handle_collection(dbname, resource_name):
    print("handle collection")
    resp = Response("Internal server error", status=500, mimetype="text/plain")
    result = None
    e = None
    context = get_context()
    # print(context)
    # print(request.args)

    try:
        children = context.get("children", None)
        if children is not None:
            resp = handle_path(dbname, resource_name, context)
        else:
            # Form the compound resource names dbschema.table_name
            resource = dbname + "." + resource_name
            if request.method == "GET":
                # Get the field list if it exists.
                field_list = request.args.get('fields', None)
                if field_list is not None:
                    field_list = field_list.split(",")
                limit = request.args.get('limit', 10)
                offset = request.args.get('offset', 0)
                order_by = request.args.get('order_by', None)
                # The query string is of the form ?f1=v1&f2=v2& ...
                # This maps to a query template of the form { "f1" : "v1", ... }
                # We need to ignore the fields parameters.
                tmp = None
                for k,v in request.args.items():
                    if (not k == 'fields') and (not k == 'limit') and (not k == 'offset') and\
                        (not k == 'order_by'):
                        if tmp is None:
                            tmp = {}
                        tmp[k] = v
                # Find by template.

                result = ds.get_by_template(resource, tmp, field_list=field_list, limit=limit, offset=offset, order_by=order_by)
                if result:
                    result = {resource: result}
                    result = compute_links(result, limit, offset)
                    result_data = json.dumps(result, default=str)
                    resp = Response(result_data, status=200, mimetype='application/json')
                else:
                    resp = Response("Not found", status=404, mimetype="text/plain")

            elif request.method == "POST":
                new_r = request.get_json()
                k = ds.create(resource, new_r)
                if k is not None:
                    location = get_location(dbname, resource_name, k)
                    resp = Response("Created", status=201, mimetype="text/plain")
                    resp.headers['Location'] = location
    except Exception as e:
        utils.debug_message("Something awful happened, e = ", e)
        print("not good")
    return resp

if __name__ == '__main__':
    app.run(debug=True)
