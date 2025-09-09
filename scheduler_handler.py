from flask import Flask, request, jsonify
import sys
import os

sys.path.append('./data-collection')
from pipeline import run_data_pipeline

import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

@app.route('/run-pipeline', methods=['POST'])
def trigger_pipeline():
    try:
        logging.info("파이프라인 실행 시작")
        success = run_data_pipeline()

        if success:
            return jsonify({'status': 'success', 'message': '파이프라인 실행 완료'}), 200
        else:
            return jsonify({'status': 'failed', 'message': '파이프라인 실행 실패'}), 500
    except Exception as e:
        logging.error(f"파이프라인 실행 중 오류 발생: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'healthy'}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    # 프로덕션 환경에서는 debug=False
    debug_mode = os.environ.get('ENV', 'production') != 'production'
    app.run(debug=debug_mode, host='0.0.0.0', port=port)
