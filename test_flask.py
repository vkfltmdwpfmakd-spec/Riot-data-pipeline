"""
간단한 Cloud Run Flask 테스트 스크립트
"""

def test_flask_app():
    print("Flask 앱 테스트 시작")
    
    print("\n1단계: Docker 이미지 빌드:")
    print("   docker build -t riot-test .")
    
    print("\n2단계: Docker 컨테이너 실행:")
    print("   docker run -d -p 8080:8080 \\")
    print("     -e RIOT_API_KEY=test \\") 
    print("     -e GOOGLE_CLOUD_PROJECT=test-project \\")
    print("     -e ENV=development \\")
    print("     --name riot-test-container riot-test")
    
    print("\n3단계: 브라우저에서 테스트:")
    print("   http://localhost:8080/health")
    print("   -> {'status':'healthy'} 나와야 함")
    
    print("\n4단계: 파이프라인 실행 테스트:")
    print("   curl -X POST http://localhost:8080/run-pipeline")
    
    print("\n5단계: 컨테이너 정리:")
    print("   docker stop riot-test-container")
    print("   docker rm riot-test-container")
    
    print("\n성공하면 Cloud Run 배포 가능!")

if __name__ == "__main__":
    test_flask_app()