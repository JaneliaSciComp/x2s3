import sys
import yaml
import os
from typing import Optional, Dict
import xml.etree.ElementTree as ET

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

    path = details['path'] if 'path' in details else None

    s3_clients[target] = {
        'client': client,
        'prefix': path
    }


def create_xml_element(parent, key, value):
    """Helper function to create an XML element from a key-value pair."""
    if isinstance(value, dict):
        elem = ET.SubElement(parent, key)
        for sub_key, sub_value in value.items():
            create_xml_element(elem, sub_key, sub_value)
    elif isinstance(value, list):
        for item in value:
            elem = ET.SubElement(parent, key)
            if isinstance(item, dict):
                for sub_key, sub_value in item.items():
                    create_xml_element(elem, sub_key, sub_value)
            else:
                elem.text = str(item)
    else:
        elem = ET.SubElement(parent, key)
        elem.text = str(value)


def create_xml_response(data):
    root = ET.Element("ListBucketResult")

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


@app.get("/{target}/")
async def list_objects_v2(request: Request,
                          target: str,
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

        # Format the response to XML
        xml_response = {
            "IsTruncated": response.get("IsTruncated", False),
            "Contents": [{
                "Key": obj["Key"],
                "LastModified": obj["LastModified"].isoformat(),
                "ETag": obj["ETag"],
                "Size": obj["Size"],
                "StorageClass": obj.get("StorageClass", ""),
                "Owner": {
                    "DisplayName": obj["Owner"]["DisplayName"] if "DisplayName" in obj["Owner"] else '',
                    "ID": obj["Owner"]["ID"]
                } if "Owner" in obj else {}
            } for obj in response.get("Contents", [])],
            "Name": bucket_name,
            "Prefix": prefix or "",
            "Delimiter": delimiter or "",
            "MaxKeys": max_keys,
            "CommonPrefixes": [{"Prefix": cp["Prefix"]} for cp in response.get("CommonPrefixes", [])],
            "EncodingType": encoding_type or "",
            "KeyCount": response.get("KeyCount", 0),
            "ContinuationToken": continuation_token or "",
            "NextContinuationToken": response.get("NextContinuationToken", ""),
            "StartAfter": start_after or "",
            "Marker": marker or "",
            "NextMarker": response.get("NextContinuationToken", "")
        }


        xml_output = create_xml_response(xml_response)
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


@app.get("/{target}/{key:path}")
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
        return StreamingResponse(response['Body'], media_type='application/octet-stream', headers={
            'Content-Disposition': f'attachment; filename="{key}"'
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



@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    bucket_list = { target: f"/{target}/?list-type=2&max-keys=1" for target in s3_clients.keys()}
    return templates.TemplateResponse("index.html", {"request": request, "links": bucket_list})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

