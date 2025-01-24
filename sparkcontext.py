import requests
import json
import os
import boto3
from abc import ABC, abstractmethod

class StorageHandler(ABC):
    @abstractmethod
    def save_records(self, records: list, file_path: str) -> None:
        pass

class LocalStorageHandler(StorageHandler):
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def save_records(self, records: list, file_name: str) -> None:
        file_path = os.path.join(self.output_dir, file_name)
        # Write the file fresh each time
        with open(file_path, 'w') as f:
            for record in records:
                json.dump(record, f)
                f.write('\n')

class S3StorageHandler(StorageHandler):
    def __init__(self, bucket: str, prefix: str):
        self.s3 = boto3.client('s3')
        self.bucket = bucket
        self.prefix = prefix

    def save_records(self, records: list, file_name: str) -> None:
        key = f"{self.prefix}/{file_name}"
        content = '\n'.join(json.dumps(record) for record in records)
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=content + '\n'
        )

class EvaluationAnalyzer:
    def __init__(self, storage_handler: StorageHandler):
        self.base_url = "https://api.mypurecloud.com/api/v2"
        self.headers = {}
        self.page_size = 100
        self.storage = storage_handler

    def set_bearer_token(self, token: str) -> None:
        self.headers = {"Authorization": f"Bearer {token}"}

    def get_active_evaluators(self, start_time: str, end_time: str) -> list:
        active_evaluators = []
        page_number = 1
        
        while True:
            url = (f"{self.base_url}/quality/evaluators/activity"
                  f"?startTime={start_time}"
                  f"&endTime={end_time}"
                  f"&pageSize={self.page_size}"
                  f"&pageNumber={page_number}")
            
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            
            if not data.get('entities', []):
                break
            
            for entity in data['entities']:
                if entity.get('numEvaluationsCompleted', 0) > 0:
                    evaluator_id = entity['evaluator']['id']
                    active_evaluators.append(evaluator_id)
            
            page_number += 1
        
        return active_evaluators

    def process_evaluation_record(self, entity: dict) -> list:
        records = []
        base_record = {
            'id': entity.get('id'),
            'conversation': entity.get('conversation'),
            'evaluationForm': entity.get('evaluationForm'),
            'evaluator': entity.get('evaluator'),
            'agent': entity.get('agent'),
            'status': entity.get('status'),
            'agentHasRead': entity.get('agentHasRead'),
            'assigneeApplicable': entity.get('assigneeApplicable'),
            'releaseDate': entity.get('releaseDate'),
            'assignedDate': entity.get('assignedDate'),
            'changedDate': entity.get('changedDate'),
            'queue': entity.get('queue'),
            'mediaType': entity.get('mediaType'),
            'conversationDate': entity.get('conversationDate'),
            'conversationEndDate': entity.get('conversationEndDate'),
            'neverRelease': entity.get('neverRelease'),
            'dateAssigneeChanged': entity.get('dateAssigneeChanged'),
            'hasAssistanceFailed': entity.get('hasAssistanceFailed'),
            'evaluationSource': entity.get('evaluationSource'),
            'disputeCount': entity.get('disputeCount'),
            'version': entity.get('version'),
            'declinedReview': entity.get('declinedReview'),
            'evaluationContextId': entity.get('evaluationContextId'),
            'aiScoring': entity.get('aiScoring')
        }

        if 'answers' in entity:
            for key, value in entity['answers'].items():
                if key != 'questionGroupScores':
                    base_record[f'answers_{key}'] = value

            for group in entity['answers'].get('questionGroupScores', []):
                group_data = base_record.copy()
                group_data.update({
                    k: v for k, v in group.items()
                    if k != 'questionScores'
                })

                for question in group.get('questionScores', []):
                    question_record = group_data.copy()
                    question_record.update(question)
                    records.append(question_record)

        return records

    def get_evaluations_data(self, evaluator_id: str, start_time: str, end_time: str) -> int:
        total_records = 0
        page_number = 1

        while True:
            url = (f"{self.base_url}/quality/evaluations/query"
                  f"?startTime={start_time}"
                  f"&endTime={end_time}"
                  f"&pageSize={self.page_size}"
                  f"&pageNumber={page_number}"
                  f"&evaluatorUserId={evaluator_id}"
                  f"&expandAnswerTotalScores=true")

            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()

            if not data.get('entities', []):
                break

            for entity in data['entities']:
                file_name = f"{entity['id']}_{entity['conversation']['id']}.jsonl"
                records = self.process_evaluation_record(entity)
                self.storage.save_records(records, file_name)
                total_records += len(records)

            page_number += 1

        return total_records

def main():
    CONFIG = {
        'start_time': '2024-10-15T00:00:00Z',
        'end_time': '2024-11-10T00:00:00Z',
        'bearer_token': 'your-bearer-token-here',
        'storage': {
            'type': 'local',  # or 's3'
            'local_path': './output1/',
            's3_bucket': 'your-bucket',
            's3_prefix': 'evaluations'
        }
    }

    storage = (S3StorageHandler(CONFIG['storage']['s3_bucket'], CONFIG['storage']['s3_prefix']) 
              if CONFIG['storage']['type'] == 's3' 
              else LocalStorageHandler(CONFIG['storage']['local_path']))

    analyzer = EvaluationAnalyzer(storage)
    analyzer.set_bearer_token(CONFIG['bearer_token'])

    evaluator_ids = analyzer.get_active_evaluators(
        start_time=CONFIG['start_time'],
        end_time=CONFIG['end_time']
    )

    for evaluator_id in evaluator_ids:
        analyzer.get_evaluations_data(
            evaluator_id=evaluator_id,
            start_time=CONFIG['start_time'],
            end_time=CONFIG['end_time']
        )

if __name__ == "__main__":
    main()
