import pandas as pd
import psycopg2
from datetime import datetime
import os
import glob


# 데이터베이스 연결 설정
def connect_database():
    # 환경 변수 또는 기본값 사용
    database = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    # PostgreSQL 연결 설정
    conn = psycopg2.connect(
        host="localhost",  # 도커 컨테이너가 로컬에서 실행 중이라면 localhost, 아니면 컨테이너 IP
        port="5432",  # docker-compose.yml에서 설정한 포트
        database=database,  # .env 파일에서 설정한 데이터베이스 이름
        user=user,  # .env 파일에서 설정한 사용자 이름
        password=password,  # .env 파일에서 설정한 비밀번호
    )

    return conn


# 테이블 생성
def create_tables(conn):
    cursor = conn.cursor()

    # 필요한 테이블 생성 쿼리들
    create_tables_queries = [
        """
        CREATE TABLE IF NOT EXISTS report_types (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) UNIQUE NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS companies (
            ticker VARCHAR(20) PRIMARY KEY,
            company_name VARCHAR(255) NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS metrics (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            report_type_id INTEGER REFERENCES report_types(id),
            UNIQUE (name, report_type_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS financial_data (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(20) REFERENCES companies(ticker),
            metric_id INTEGER REFERENCES metrics(id),
            value TEXT NOT NULL,
            report_date DATE NOT NULL,
            UNIQUE (ticker, metric_id, report_date)
        );
        """,
    ]

    for query in create_tables_queries:
        cursor.execute(query)

    conn.commit()
    cursor.close()

    print("테이블 생성 완료")


# 인덱스 생성
def create_indexes(conn):
    cursor = conn.cursor()

    # 쿼리 성능 향상을 위한 인덱스 생성
    create_indexes_queries = [
        """
        CREATE INDEX IF NOT EXISTS idx_financial_data_ticker ON financial_data(ticker);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_financial_data_metric_id ON financial_data(metric_id);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_financial_data_report_date ON financial_data(report_date);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_metrics_report_type_id ON metrics(report_type_id);
        """,
    ]

    for query in create_indexes_queries:
        cursor.execute(query)

    conn.commit()
    cursor.close()

    print("인덱스 생성 완료")


# Excel 데이터 로드 및 처리
def load_excel_data(conn):
    cursor = conn.cursor()

    # 데이터 디렉토리 경로
    data_dir = "./data"  # 컨테이너 내부 경로

    # Excel 파일 찾기
    excel_files = {
        "balance_sheet": glob.glob(f"{data_dir}/*balance_sheet*.xlsx"),
        "cash_flow": glob.glob(f"{data_dir}/*cash_flow*.xlsx"),
        "financial_ratios": glob.glob(f"{data_dir}/*financial_ratios*.xlsx"),
        "income_statement": glob.glob(f"{data_dir}/*income_statement*.xlsx"),
    }

    # 파일 경로가 실제로 존재하는지 확인
    for report_type, file_paths in excel_files.items():
        if not file_paths:
            print(f"경고: {report_type} 유형의 Excel 파일을 찾을 수 없습니다.")
        else:
            # 첫 번째 파일 사용
            excel_files[report_type] = file_paths[0]
            print(f"'{report_type}' 유형의 파일을 찾았습니다: {file_paths[0]}")

    # 보고서 유형 ID 가져오기
    cursor.execute("SELECT id, name FROM report_types")
    report_type_ids = {name: id for id, name in cursor.fetchall()}

    # 보고서 유형이 없으면 생성
    for report_type in excel_files.keys():
        if report_type not in report_type_ids:
            cursor.execute(
                """
                INSERT INTO report_types (name) 
                VALUES (%s) 
                ON CONFLICT (name) DO NOTHING
                RETURNING id
                """,
                (report_type,),
            )
            result = cursor.fetchone()
            if result:
                report_type_ids[report_type] = result[0]
            else:
                cursor.execute(
                    "SELECT id FROM report_types WHERE name = %s", (report_type,)
                )
                result = cursor.fetchone()
                if result:
                    report_type_ids[report_type] = result[0]
                else:
                    print(f"오류: {report_type} 유형의 보고서 ID를 가져올 수 없습니다.")
                    continue

    # 현재 날짜를 보고서 날짜로 사용
    report_date = datetime.now().date()

    # 개별 보고서 파일 처리
    for report_type, file_path in excel_files.items():
        # 파일 경로가 문자열인지 확인
        if not isinstance(file_path, str):
            print(
                f"경고: {report_type} 유형의 파일 경로가 유효하지 않습니다. 건너뜁니다."
            )
            continue

        print(f"{report_type} 파일 처리 중...")

        # 파일이 존재하는지 확인
        if not os.path.exists(file_path):
            print(f"경고: {file_path} 파일이 존재하지 않습니다. 건너뜁니다.")
            continue

        try:
            # Excel 파일 로드
            df = pd.read_excel(file_path)

            # 첫번째 열을 인덱스로 설정 (회사 정보가 있는 열)
            df.set_index(df.columns[0], inplace=True)

            report_type_id = report_type_ids[report_type]

            # 지표 등록
            for col in df.columns:
                cursor.execute(
                    """
                    INSERT INTO metrics (name, report_type_id) 
                    VALUES (%s, %s) 
                    ON CONFLICT (name, report_type_id) DO NOTHING
                    RETURNING id
                    """,
                    (col, report_type_id),
                )

                result = cursor.fetchone()
                if result:
                    metric_id = result[0]
                else:
                    cursor.execute(
                        "SELECT id FROM metrics WHERE name = %s AND report_type_id = %s",
                        (col, report_type_id),
                    )
                    metric_id = cursor.fetchone()[0]

                # 각 기업에 대한 데이터 추가
                for idx, value in df[col].items():
                    # 'AAPL (apple)' 형식에서 티커와 회사명 추출
                    if " (" in idx and ")" in idx:
                        parts = idx.split(" (")
                        ticker = parts[0].strip()
                        company_name = parts[1].replace(")", "").strip()

                        # 기업 정보 등록
                        cursor.execute(
                            """
                            INSERT INTO companies (ticker, company_name) 
                            VALUES (%s, %s) 
                            ON CONFLICT (ticker) DO UPDATE SET company_name = EXCLUDED.company_name
                            """,
                            (ticker, company_name),
                        )

                        # 값이 NaN이 아닌 경우만 저장
                        if pd.notna(value):
                            # 값을 문자열로 저장하여 원래 형식 유지
                            value_str = str(value)

                            cursor.execute(
                                """
                                INSERT INTO financial_data (ticker, metric_id, value, report_date) 
                                VALUES (%s, %s, %s, %s) 
                                ON CONFLICT (ticker, metric_id, report_date) DO UPDATE SET value = EXCLUDED.value
                                """,
                                (ticker, metric_id, value_str, report_date),
                            )
        except Exception as e:
            print(f"에러: {file_path} 처리 중 오류 발생: {str(e)}")
            continue

    # 데이터베이스 변경사항 저장
    conn.commit()
    cursor.close()

    print("데이터 가져오기 완료")

def get_company_financial_data(conn, ticker):
    """특정 기업의 모든 재무 데이터 조회"""
    cursor = conn.cursor()

    # 기업 정보 확인
    cursor.execute(
        "SELECT ticker, company_name FROM companies WHERE ticker = %s", (ticker,)
    )
    company = cursor.fetchone()

    if not company:
        return None

    ticker, company_name = company

    # 재무 데이터 조회
    cursor.execute(
        """
        SELECT rt.name as report_type, m.name as metric, fd.value
        FROM financial_data fd
        JOIN metrics m ON fd.metric_id = m.id
        JOIN report_types rt ON m.report_type_id = rt.id
        WHERE fd.ticker = %s
        ORDER BY rt.name, m.name
    """,
        (ticker,),
    )

    results = cursor.fetchall()

    # 결과 포맷팅
    formatted_data = {"ticker": ticker, "company_name": company_name, "financials": {}}

    for report_type, metric, value in results:
        if report_type not in formatted_data["financials"]:
            formatted_data["financials"][report_type] = {}

        formatted_data["financials"][report_type][metric] = value

    return formatted_data

# 메인 함수
def main():
    # 데이터베이스 연결
    conn = connect_database()

    try:
        # 테이블 생성
        create_tables(conn)

        # Excel 데이터 로드
        load_excel_data(conn)

        # 인덱스 생성
        create_indexes(conn)

        print("모든 작업이 완료되었습니다.")
    except Exception as e:
        print(f"오류 발생: {str(e)}")
    finally:
        # 연결 종료
        conn.close()


if __name__ == "__main__":
    main()
