from flask import Flask, request, jsonify
import requests
from requests.auth import HTTPBasicAuth
import os

app = Flask(__name__)

CO_DOMAIN = "https://codeocean.allenneuraldynamics.org"
CAPSULE_ID = "576015ec-10ec-45c1-a095-3ec2721feae3"

# Secure: stored in Render environment variables
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")


# ---- Route ----
@app.route("/run-job")
def run_job():
    batch = request.args.get("batch_name")
    workflow = request.args.get("workflow")
    fastq = request.args.get("fastq_name")

    payload = {
        "capsule_id": CAPSULE_ID,
        "named_parameters": [
            {"param_name": "workflow", "value": workflow},
            {"param_name": "batch-name", "value": batch},
            {"param_name": "fastq-name", "value": fastq},
            {"param_name": "email", "value": "!BICore@alleninstitute.org"},
            {"param_name": "dry-run", "value": "false"},
            {"param_name": "debug", "value": "false"}
        ]
    }

    response = requests.post(
        f"{CO_DOMAIN}/api/v1/computations",
        auth=HTTPBasicAuth(ACCESS_TOKEN, ""),
        json=payload
    )

    return jsonify({
        "status_code": response.status_code,
        "response": response.json(),
        "payload_sent": payload
    })
