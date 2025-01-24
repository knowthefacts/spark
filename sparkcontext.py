import json
import boto3
import requests
import os
from typing import List, Dict
import logging
from abc import ABC, abstractmethod

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StorageHandler(ABC):
    @abstractmethod
    def save(self, content: str, filepath: str) -> str:
        pass

class LocalStorageHandler(StorageHandler):
    def __init__(self, base_path: str):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)

    def save(self, content: str, filepath: str) -> str:
        full_path = os.path.join(self.base_path, filepath)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return full_path

class S3StorageHandler(StorageHandler):
    def __init__(self, bucket: str, prefix: str):
        self.bucket = bucket
        self.prefix = prefix.rstrip('/')
        self.s3_client = boto3.client('s3')

    def save(self, content: str, filepath: str) -> str:
        s3_key = f"{self.prefix}/{filepath}"
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=content.encode('utf-8')
        )
        return f"s3://{self.bucket}/{s3_key}"

class GenesysProcessor:
    def __init__(self, base_api_url: str, storage_handler: StorageHandler, secret_name: str = None, bearer_token: str = None):
        self.base_api_url = base_api_url
        self.storage_handler = storage_handler
        self.bearer_token = bearer_token if bearer_token else self._get_bearer_token(secret_name)

    def _get_bearer_token(self, secret_name: str) -> str:
        try:
            secrets_client = boto3.client('secretsmanager')
            response = secrets_client.get_secret_value(SecretId=secret_name)
            secrets = json.loads(response['SecretString'])
            return secrets.get('bearer_token')
        except Exception as e:
            logger.error(f"Failed to fetch bearer token: {e}")
            raise

    def _make_request(self, url: str) -> Dict:
        headers = {'Authorization': f'Bearer {self.bearer_token}'} if self.bearer_token else {}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed API request to {url}: {e}")
            raise

    def fetch_form_list(self) -> Dict:
        url = f"{self.base_api_url}/quality/forms/evaluations?pageSize=100&expand=publishHistory"
        return self._make_request(url)

    def fetch_form_detail(self, form_id: str) -> Dict:
        url = f"{self.base_api_url}/quality/forms/evaluations/{form_id}?expand=publishHistory"
        return self._make_request(url)

    def prefix_dict_keys(self, d: Dict, prefix: str) -> Dict:
        return {f"{prefix}_{k}": v for k, v in d.items()}

    def process_question_groups(self, form_entity: Dict):
        form_id = form_entity.get('id')
        detailed_form = self.fetch_form_detail(form_id)
        question_groups = detailed_form.get('questionGroups', [])
        answer_options_data = []

        for qg in question_groups:
            qg_data = self.prefix_dict_keys(qg, 'questiongroups')
            qg_data['formid'] = form_id
            
            questions = qg.pop('questiongroups_questions', [])
            
            for question in questions:
                q_data = self.prefix_dict_keys(question, 'questions')
                answer_options = question.pop('questions_answerOptions', [])
                combined_data = {**qg_data, **q_data}
                
                for ao in answer_options:
                    ao_data = self.prefix_dict_keys(ao, 'answeroptions')
                    record = {**combined_data, **ao_data}
                    answer_options_data.append(record)

        if answer_options_data:
            filename = f"questiongroups_{form_id}.jsonl"
            jsonl_content = '\n'.join(json.dumps(item) for item in answer_options_data)
            self.storage_handler.save(jsonl_content, filename)
            logger.info(f"Saved question groups data for form {form_id}")

    def process_entities(self, data: Dict) -> List[Dict]:
        processed_entities = []
        for root_entity in data.get('entities', []):
            published_versions = root_entity.get('publishedVersions', {})
            if not published_versions:
                continue
            for entity in published_versions.get('entities', []):
                processed_entity = {
                    'id': entity.get('id'),
                    'name': entity.get('name'),
                    'modifiedDate': entity.get('modifiedDate'),
                    'published': entity.get('published'),
                    'contextId': entity.get('contextId'),
                    'weightMode': entity.get('weightMode'),
                    'selfUri': entity.get('selfUri')
                }
                processed_entities.append(processed_entity)
                self.process_question_groups(entity)
        return processed_entities

    def save_to_jsonl(self, entities: List[Dict], output_file: str) -> str:
        jsonl_content = '\n'.join(json.dumps(entity) for entity in entities)
        return self.storage_handler.save(jsonl_content, output_file)

def main():
    BASE_API_URL = "https://api.mypurecloud.com/api/v2"
    STORAGE_TYPE = "local"
    OUTPUT_FILE = "forms.jsonl"
    
    SECRET_NAME = "your-secret-name"
    BEARER_TOKEN = "your-bearer-token"

    storage_handler = LocalStorageHandler("./output") if STORAGE_TYPE == "local" else S3StorageHandler("your-bucket-name", "genesys/forms")

    try:
        processor = GenesysProcessor(
            BASE_API_URL,
            storage_handler,
            secret_name=SECRET_NAME,  # Comment this if using direct token
            # bearer_token=BEARER_TOKEN  # Uncomment if using direct token
        )
        raw_data = processor.fetch_form_list()
        processed_entities = processor.process_entities(raw_data)
        output_path = processor.save_to_jsonl(processed_entities, OUTPUT_FILE)
        logger.info(f"Successfully exported {len(processed_entities)} entities to {output_path}")
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

if __name__ == "__main__":
    main()
