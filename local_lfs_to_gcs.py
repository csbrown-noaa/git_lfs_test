#!/usr/bin/env python

import os
import sys
import json
import subprocess
import requests
import time
import asyncio

def get_temporary_path(oid):
    """Compute the temporary path for a file based on its OID."""
    base_dir = ".git/lfs/objects"
    subdir = os.path.join(oid[:2], oid[2:4])
    return os.path.join(base_dir, subdir, oid)

class GCSLfsTransferAgent:
    def __init__(self):
        self.public_url = "https://storage.googleapis.com/{bucket}/PIFSC%2FSOD%2Fgit_lfs_test%2F{object_name}"
        self.private_url = "https://storage.googleapis.com/storage/v1/b/{bucket}/o/PIFSC%2FSOD%2Fgit_lfs_test%2F{object_name}?alt=media"
        self.upload_url = "https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o?uploadType=media&name=PIFSC%2FSOD%2Fgit_lfs_test%2F{object_name}"

    def download(self, bucket, object_name, destination_path):
        """Downloads a file from GCS, handling both public and private buckets."""
        public_url = self.public_url.format(bucket=bucket, object_name=object_name)
        private_url = self.private_url.format(bucket=bucket, object_name=object_name)

        # Try downloading as a public object
        response = requests.get(public_url, stream=True)
        
        temp_path = get_temporary_path(object_name)
        if response.status_code == 200:
            self._write_to_file(response, temp_path)
            sys.stdout.write(
                json.dumps({"event": "complete", "oid": object_name, "path": destination_path}) + "\n"
            )
            sys.stdout.flush()
            return

        # If the public download fails, try with authentication
        if response.status_code == 403:
            headers = {
                "Authorization": f"Bearer {self.get_access_token()}",
            }
            response = requests.get(private_url, headers=headers, stream=True)
            if response.status_code == 200:
                self._write_to_file(response, destination_path)
                sys.stdout.write(
                    json.dumps({"event": "complete", "oid": object_name, "path": destination_path}) + "\n"
                )
                sys.stdout.flush()
                return

        # Raise an error if the download fails
        raise RuntimeError(f"Error downloading file: {response.status_code} {response.text}")

    async def handle_download(self, payload):
        """Handle the download event."""
        oid = payload.get("oid")
        path = payload.get("path")
        bucket = os.getenv("GCS_BUCKET")
        sys.stderr.write(f"Handling download for OID: {oid}, Path: {path}\n")
        self.download(bucket, oid, path)
    
    def _write_to_file(self, response, destination_path):
        """Writes the response content to a file."""
        with open(destination_path, "wb") as file_data:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # Filter out keep-alive new chunks
                    file_data.write(chunk)

    def get_access_token(self):
        """Fetches the access token from gcloud."""
        result = subprocess.run(
            ["gcloud", "auth", "print-access-token"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Error fetching access token: {result.stderr}")
        return result.stdout.strip()

    async def handle_init(self, payload):
        """Handle the init event from Git LFS."""
        sys.stderr.write(f"Handling init: {payload}\n")
        sys.stderr.flush()
        sys.stdout.write(json.dumps({"event": "init"}) + "\n")
        sys.stdout.flush()
        sys.stderr.write("Init event processed and response flushed\n")

    async def handle_terminate(self, payload):
        """Handle the terminate event."""
        sys.stderr.write("Handling terminate event\n")
        sys.stdout.write(json.dumps({"event": "terminate"}) + "\n")
        sys.stdout.flush()
        sys.stderr.write("Terminate event processed\n")
        sys.stderr.flush()
        #sys.exit(0)
    
    def upload(self, bucket, object_name, file_path):
        """Uploads a file to GCS using the REST API."""
        sys.stderr.write("uploading file")
        sys.stderr.flush()

        upload_url = self.upload_url.format(bucket=bucket, object_name=object_name)

        headers = {
            "Content-Type": "application/octet-stream",
            "Authorization": "Bearer {}".format(self.get_access_token())
        }
        with open(file_path, "rb") as file_data:
            response = requests.post(upload_url, headers=headers, data=file_data)
        
        sys.stderr.write(f"response received: {response}")
        sys.stderr.flush()

        if response.status_code == 200:
            sys.stderr.write(f"completing upload event")
            sys.stderr.flush()
            sys.stderr.write(f"{response.json()}")
            sys.stderr.flush()
            sys.stdout.write(
              json.dumps({"event": "complete", "oid": object_name, "size": os.path.getsize(file_path)}) + "\n"
            )
            sys.stdout.flush()
        else:
            raise RuntimeError(f"Error uploading file: {response.status_code} {response.text}")
        sys.stderr.write(f"upload event concluded")
        sys.stderr.flush()
    
    async def handle_upload(self, payload):
        """Handle the upload event."""
        oid = payload.get("oid")
        path = payload.get("path")
        bucket = os.getenv("GCS_BUCKET")
        sys.stderr.write(f"Handling upload for OID: {oid}, Path: {path}\n")
        self.upload(bucket, oid, path)


    async def handle_request(self):
        """Main entry point for the transfer agent."""
        sys.stderr.write("Transfer agent started\n")
        reader = asyncio.StreamReader()
        loop = asyncio.get_event_loop()
        await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)

        async for line in reader:
            if not line.strip():
                continue
            try:
                payload = json.loads(line.strip())
                await self.process_event(payload)
            except json.JSONDecodeError as e:
                sys.stderr.write(f"Error decoding JSON: {e}\n")
                break

    async def process_event(self, payload):
        """Route events to their handlers."""
        event = payload.get("event")
        sys.stderr.write(f"Processing event: {event}\n")
        if event == "init":
            await self.handle_init(payload)
        elif event == "upload":
            await self.handle_upload(payload)
        elif event == "download":
            await self.handle_download(payload)
        elif event == "terminate":
            await self.handle_terminate(payload)
        else:
            sys.stderr.write(f"Unsupported event: {event}\n")




if __name__ == "__main__":
    sys.stderr.write("Transferring\n")
    sys.stderr.flush()
    agent = GCSLfsTransferAgent()
    #agent.handle_request()
    asyncio.run(agent.handle_request())
    sys.stderr("finished\n")
    sys.stderr.flush()

