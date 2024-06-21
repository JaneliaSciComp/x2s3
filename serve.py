import sys
import yaml
import os
from typing import Optional, Dict
import xml.etree.ElementTree as ET
from functools import partial

from loguru import logger
import boto3
import botocore
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import Response, StreamingResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware

# For debugging
#boto3.set_stream_logger(name='botocore')

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
templates = Jinja2Templates(directory="templates")

# Load target bucket configurations from a YAML file
CONFIG_FILE = 'config.yaml'
with open(CONFIG_FILE, 'r') as file:
    config = yaml.safe_load(file)

# Initialize S3 clients for each target
s3_clients = {}

for target, details in config['targets'].items():
    anonymous = True
    access_key,secret_key = '',''

    if 'credentials' in details:
        anonymous = False
        access_key_path = details['credentials']['accessKeyPath']
        secret_key_path = details['credentials']['secretKeyPath']

        with open(access_key_path, 'r') as ak_file:
            access_key = ak_file.read().strip()

        with open(secret_key_path, 'r') as sk_file:
            secret_key = sk_file.read().strip()

    session_config = botocore.config.Config(
        user_agent="jproxy"
    )

    client = boto3.client(
        's3',
        config=session_config,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=details['endpoint']
    )

    if anonymous:
        # hack to disable signing: https://stackoverflow.com/questions/34865927/can-i-use-boto3-anonymously
        client._request_signer.sign = (lambda *args, **kwargs: None)

    prefix = details['prefix'] if 'prefix' in details else None

    s3_clients[target] = {
        'client': client,
        'prefix': prefix
    }


def create_xml_element(parent, key, value):
    """Helper function to create an XML element from a key-value pair."""
    if isinstance(value, dict):
        for sub_key, sub_value in value.items():
            create_xml_element(parent, sub_key, sub_value)
    elif isinstance(value, list):
        elem = ET.SubElement(parent, key)
        for item in value:
            create_xml_element(elem, key, item)
    else:
        elem = ET.SubElement(parent, key)
        elem.text = str(value)


def create_xml_response(root_element_name, data):
    root = ET.Element(root_element_name)
    for key, value in data.items():
        create_xml_element(root, key, value)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def get_bucket_acl(request: Request, target: str):
    # Full read access
    acl_xml = """
    <AccessControlPolicy>
        <Owner>
            <ID>1</ID>
            <DisplayName>unknown</DisplayName>
        </Owner>
        <AccessControlList>
            <Grant>
                <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
                    <URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>
                </Grantee>
                <Permission>READ</Permission>
            </Grant>
        </AccessControlList>
    </AccessControlPolicy>
    """
    return Response(content=acl_xml, media_type="application/xml")


def remove_prefix(client_prefix, key):
    if key and client_prefix:
        return key.removeprefix(client_prefix).removeprefix('/')
    return key


async def browse_bucket(request: Request, target: str, prefix: str):
    if target not in s3_clients:
        raise HTTPException(status_code=404, detail="Target bucket not found")

    s3_client_obj = s3_clients[target]
    s3_client = s3_client_obj['client']
    client_prefix = s3_client_obj['prefix']
    bucket_name = config['targets'][target]['bucket']
    
    if prefix and not prefix.endswith('/'):
        prefix += '/'

    real_prefix = prefix
    if client_prefix:
        real_prefix = os.path.join(client_prefix, prefix) if prefix else client_prefix

    rm_prefix = partial(remove_prefix, real_prefix)
    parent_prefix = os.path.dirname(prefix.rstrip('/'))

    try:
        params = {"Bucket": bucket_name, "Prefix": real_prefix, "Delimiter": "/", "MaxKeys": 10}
        response = s3_client.list_objects_v2(**params)

        common_prefixes = [prefix["Prefix"] for prefix in response.get("CommonPrefixes", [])]
        contents = [{"key": obj["Key"]} for obj in response.get("Contents", []) if obj["Key"] != prefix]

        return templates.TemplateResponse("browse.html", {
            "request": request,
            "bucket_name": bucket_name,
            "prefix": prefix,
            "target": target,
            "common_prefixes": common_prefixes,
            "contents": contents,
            "parent_prefix": parent_prefix,
            "rm_prefix": rm_prefix
        })

    except (NoCredentialsError, PartialCredentialsError):
        raise HTTPException(status_code=500, detail="AWS credentials not configured properly.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def get_object(request: Request, target: str, key: str):
    if target not in s3_clients:
        raise HTTPException(status_code=404, detail="Target bucket not found")

    s3_client_obj = s3_clients[target]
    s3_client = s3_client_obj['client']
    client_prefix = s3_client_obj['prefix']
    bucket_name = config['targets'][target]['bucket']

    if client_prefix:
        key = os.path.join(client_prefix, key) if key else client_prefix

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        filename = os.path.basename(key)
        return StreamingResponse(response['Body'], media_type='application/octet-stream', headers={
            'Content-Disposition': f'attachment; filename="{filename}"'
        })
    except s3_client.exceptions.NoSuchKey:
        logger.info(f"Object not found: {key}")
        raise HTTPException(status_code=404, detail="Object not found")
    except (NoCredentialsError, PartialCredentialsError):
        logger.opt(exception=sys.exc_info()).info("AWS credentials not configured properly")
        raise HTTPException(status_code=500, detail="AWS credentials not configured properly")
    except botocore.exceptions.ReadTimeoutError as e:
        raise HTTPException(status_code=408, detail="Upstream endpoint timed out")
    except botocore.exceptions.ClientError as e:
        logger.opt(exception=sys.exc_info()).info("Error getting object")
        code = e.response['ResponseMetadata']['HTTPStatusCode']
        raise HTTPException(status_code=code, detail=str(e))
    except Exception as e:
        logger.opt(exception=sys.exc_info()).info("Error getting object")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/{target}/{path:path}")
async def list_objects_v2(request: Request,
                          target: str,
                          path: str,
                          browse: str = Query(None),
                          acl: str = Query(None),
                          list_type: int = Query(2, alias="list-type"),
                          continuation_token: Optional[str] = Query(None, alias="continuation-token"),
                          delimiter: Optional[str] = Query(None, alias="delimiter"),
                          encoding_type: Optional[str] = Query(None, alias="encoding-type"),
                          fetch_owner: Optional[bool] = Query(None, alias="fetch-owner"),
                          max_keys: Optional[int] = Query(1000, alias="max-keys"),
                          prefix: Optional[str] = Query(None, alias="prefix"),
                          marker: Optional[str] = Query(None, alias="marker"),
                          start_after: Optional[str] = Query(None, alias="start-after")):

    if browse is not None:
        return await browse_bucket(request, target, path)

    if path:
        return await get_object(request, target, path)

    if acl is not None:
        return get_bucket_acl(request, target)

    if list_type != 2:
        raise HTTPException(status_code=400, detail="Invalid list type")

    if target not in s3_clients:
        raise HTTPException(status_code=404, detail="Target bucket not found")

    s3_client_obj = s3_clients[target]
    s3_client = s3_client_obj['client']
    client_prefix = s3_client_obj['prefix']
    bucket_name = config['targets'][target]['bucket']

    if client_prefix:
        prefix = os.path.join(client_prefix, prefix) if prefix else client_prefix

    try:
        params = {"Bucket": bucket_name}
        if continuation_token is not None:
            params["ContinuationToken"] = continuation_token
        if delimiter is not None:
            params["Delimiter"] = delimiter
        if encoding_type is not None:
            params["EncodingType"] = encoding_type
        if fetch_owner is not None:
            params["FetchOwner"] = fetch_owner
        if max_keys is not None:
            params["MaxKeys"] = max_keys
        if prefix is not None:
            params["Prefix"] = prefix
        if start_after is not None:
            params["StartAfter"] = start_after
        if marker is not None:
            params["Marker"] = marker

        response = s3_client.list_objects_v2(**params)
        res_prefix = remove_prefix(client_prefix, prefix)

        contents = []
        for obj in response.get("Contents", []):
            contents.append({
                "Key": remove_prefix(client_prefix, obj["Key"]),
                "LastModified": obj["LastModified"].isoformat(),
                "ETag": obj["ETag"],
                "Size": obj["Size"],
                "StorageClass": obj.get("StorageClass", ""),
                "Owner": {
                    "DisplayName": obj["Owner"]["DisplayName"] if "DisplayName" in obj["Owner"] else '',
                    "ID": obj["Owner"]["ID"]
                } if "Owner" in obj else {}
            })

        next_token = remove_prefix(client_prefix, response.get("NextContinuationToken", ""))

        common_prefixes = []
        for cp in response.get("CommonPrefixes", []):
            common_prefix = remove_prefix(client_prefix, cp["Prefix"])
            common_prefixes.append({"Prefix": common_prefix})

        # Format the response to XML
        xml_response = {
            "IsTruncated": response.get("IsTruncated", False),
            "Contents": contents,
            "Name": target,
            "Prefix": res_prefix or "",
            "Delimiter": delimiter or "",
            "MaxKeys": max_keys,
            "CommonPrefixes": common_prefixes,
            "EncodingType": encoding_type or "",
            "KeyCount": response.get("KeyCount", 0),
            "ContinuationToken": continuation_token or "",
            "NextContinuationToken": next_token,
            "StartAfter": start_after or "",
            "Marker": marker or "",
            "NextMarker": next_token
        }

        xml_output = create_xml_response("ListBucketResult", xml_response)
        return Response(content=xml_output, media_type="application/xml")

    except (NoCredentialsError, PartialCredentialsError) as e:
        logger.opt(exception=sys.exc_info()).info("AWS credentials not configured properly")
        raise HTTPException(status_code=500, detail="AWS credentials not configured properly")
    except botocore.exceptions.ReadTimeoutError as e:
        raise HTTPException(status_code=408, detail="Upstream endpoint timed out")
    except botocore.exceptions.ClientError as e:
        logger.opt(exception=sys.exc_info()).info("Error getting list")
        code = e.response['ResponseMetadata']['HTTPStatusCode']
        raise HTTPException(status_code=code, detail=str(e))
    except Exception as e:
        logger.opt(exception=sys.exc_info()).info("Error getting list")
        raise HTTPException(status_code=500, detail=str(e))


@app.head("/{target}/{key:path}")
async def head_object(request: Request, target: str, key: str):
    if target not in s3_clients:
        raise HTTPException(status_code=404, detail="Target bucket not found")

    s3_client_obj = s3_clients[target]
    s3_client = s3_client_obj['client']
    client_prefix = s3_client_obj['prefix']
    bucket_name = config['targets'][target]['bucket']

    if client_prefix:
        key = os.path.join(client_prefix, key) if key else client_prefix

    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=key)
        return {}
    except s3_client.exceptions.NoSuchKey:
        logger.info(f"Object not found: {key}")
        raise HTTPException(status_code=404, detail="Object not found")
    except (NoCredentialsError, PartialCredentialsError):
        logger.opt(exception=sys.exc_info()).info("AWS credentials not configured properly")
        raise HTTPException(status_code=500, detail="AWS credentials not configured properly")
    except botocore.exceptions.ReadTimeoutError as e:
        logger.error("Upstream endpoint timed out")
        raise HTTPException(status_code=408, detail="Upstream endpoint timed out")
    except botocore.exceptions.ClientError as e:
        logger.opt(exception=sys.exc_info()).info("Error checking object")
        code = e.response['ResponseMetadata']['HTTPStatusCode']
        raise HTTPException(status_code=code, detail=str(e))
    except Exception as e:
        logger.opt(exception=sys.exc_info()).info("Error checking object")
        raise HTTPException(status_code=500, detail=str(e))





@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    bucket_list = { target: f"/{target}/?browse" for target in s3_clients.keys()}
    return templates.TemplateResponse("index.html", {"request": request, "links": bucket_list})



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

