# Retrieval Docs Agent

문서 검색을 위한 에이전트 기반 시스템입니다.

## 프로젝트 개요

이 프로젝트는 문서 데이터를 검색하고 관련 정보를 추출하는 에이전트를 구현합니다. PostgreSQL 데이터베이스를 활용하여 효율적인 문서 관리 및 검색 기능을 제공합니다.

## 주요 기능

- **문서 데이터 크롤링**: macrotrends에서 데이터를 수집합니다.
- **데이터베이스 연동**: PostgreSQL을 사용하여 문서 데이터를 저장하고 관리합니다.
- **검색 에이전트**: 사용자 쿼리에 대응하여 관련 문서를 효율적으로 검색합니다.

## 시작하기

### 필요 조건

- Python 3.11 이상
- Poetry (의존성 관리)
- Docker 및 Docker Compose (PostgreSQL 실행용)

### 설치 방법

1. 저장소 클론:
git clone https://github.com/ehdtjr/retrieval_docs_agent.git
cd retrieval_docs_agent

2. Poetry로 의존성 설치:
poetry install

3. Docker Compose로 PostgreSQL 실행:
docker-compose up -d

### 프로젝트 구조
```bash
retrieval_docs_agent/
├── .gitignore
├── __pycache__/
├── crawling_macrotrends.ipynb  # 데이터 크롤링 노트북
├── docker-compose.yml          # Docker 설정 파일
├── main.ipynb                  # 메인 어플리케이션 노트북
├── poetry.lock                 # Poetry 의존성 잠금 파일
├── postgresql.py               # PostgreSQL 연결 및 쿼리 로직
├── pyproject.toml              # 프로젝트 설정 및 의존성
└── tool.py                     # 유틸리티 및 도구 함수
```