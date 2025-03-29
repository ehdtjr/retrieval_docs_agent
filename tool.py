from typing import Annotated

from langchain_core.tools import tool

from postgresql import connect_database, get_company_financial_data


@tool
def retrieve_docs_tool(
    ticker: Annotated[str, "Ticker required for financial statement inquiry"],
):
    """Connect to DB and query financial statement data for ticker"""
    try:
        conn = connect_database()
        financial_data = get_company_financial_data(conn, ticker)
        # 데이터가 있는 경우에만 처리
        if financial_data:
            print(f"{ticker} 기업의 재무 데이터를 찾았습니다.")
        else:
            print(f"{ticker} 기업의 재무 데이터를 찾을 수 없습니다.")
    except BaseException as e:
        return f"Failed to execute code. Error: {repr(e)}"
    finally:
        # 사용 후 연결 닫기
        conn.close()
    return financial_data
