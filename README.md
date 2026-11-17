# kospi-data

맥미니에서 매일 장 마감 후 코스피 전종목 데이터를 모아 이 비공개 저장소에 올립니다.
클라우드 Claude(08:10 코스피 브리핑)는 이 저장소를 받아 전종목 스크리닝을 합니다.

## 데이터 출처

| 순서 | 출처 | 받는 항목 | 조건 |
|---|---|---|---|
| 1 | 한국투자증권 Open API (KIS) | 일봉·거래대금·상장주식수·투자자별 순매수(종목·시장)·코스피 지수 | `.env`의 KIS 키 (읽기 전용 키 권장) |
| 2 | pykrx | 일봉 | `.env`에 `KRX_ID`, `KRX_PW`가 있을 때만. 2025년 말부터 KRX 로그인 없이는 작동하지 않습니다 |
| 3 | FinanceDataReader | 일봉 (거래대금 없음) | 항상 |

- 연결 오류·한도 초과는 지수 백오프로 최대 5번 재시도합니다. 요청 사이에 0.1초 쉽니다.
- 휴장일은 코스피 지수에 데이터가 없는 날로 판단해 건너뜁니다.
- 15:40 전에 실행하면 오늘은 빼고 어제까지만 받습니다(장중 미완성 데이터 방지).
- 종목: KIS 종목 마스터의 코스피 주식(보통주·우선주·리츠·인프라·DR·외국주권). ETF·ETN·펀드는 제외합니다.

## 저장 구조

| 경로 | 내용 | GitHub |
|---|---|---|
| `data/daily/YYYY-MM-DD.csv` | 그날의 전종목 (UTF-8) | 올림 |
| `data/market.csv` | 코스피 지수 일봉 + 시장 전체 투자자별 순매수 | 올림 |
| `data/meta.json` | 마지막 수집일, 종목 수, 오류 목록 | 올림 |
| `data/kospi.sqlite` | 누적 테이블 `prices`, `marketcap`, `investor`, `index` | 올리지 않음 (.gitignore) |

### `data/daily/YYYY-MM-DD.csv` 열

| 열 | 뜻 |
|---|---|
| `open` `high` `low` `close` | 원주가(수정주가 아님), 원 |
| `volume` `value` | 거래량(주), 거래대금(원) |
| `marcap` `shares` | 시가총액(원), 상장주식수 |
| `marcap_approx` | 1이면 추정값: 수집 시점 상장주식수 × 그날 종가. 초기 수집의 과거 날짜가 여기에 해당합니다. 일일 수집 날짜는 0(확정) |
| `foreign_net_*` `inst_net_*` `indiv_net_*` | 외국인·기관·개인 순매수. `_value`는 원, `_qty`는 주 |
| `source` | 일봉 출처 (KIS / pykrx / FDR) |

`market.csv`의 순매수 열도 같은 단위(원, 주)입니다.

### 알려진 한계

- 종목 목록은 수집 시점 기준입니다. 2025년 이후 상장폐지된 종목은 과거 날짜에도 없습니다.
- 가격은 원주가라서 액면분할 전후로 끊깁니다. 장기 수익률을 계산할 때 주의하세요.
- FDR로 대체된 날은 거래대금·시가총액이 비어 있을 수 있습니다.

## 설치 (맥미니)

```zsh
cd ~/Projects/kospi-data
./setup.sh            # 처음에는 .env를 만들고 멈춥니다. KIS 키를 채운 뒤 다시 실행합니다.
```

`setup.sh`가 하는 일:

1. `.venv`를 만들고 `requirements.txt`를 설치합니다.
2. `.env`에 KIS 키가 있는지 확인합니다.
3. git 저장소를 만들고, gh CLI로 비공개 저장소 `kospi-data`를 만들어 연결합니다.
4. Dagu 작업 `kospi-collector`를 등록합니다(평일 16:40, `kis` 큐). 원본은 `dagu/kospi-collector.yaml`입니다.

맥미니 예약 작업은 launchd·crontab이 아니라 Dagu(중앙 통제소)에 등록합니다.
`kis` 큐는 같은 KIS 키를 쓰는 작업이 동시에 돌지 않게 합니다.

초기 수집(2025-01-01 ~ 오늘, 약 2시간 반):

```zsh
nohup .venv/bin/python collector.py --mode initial --push > logs/initial.out 2>&1 &
```

## 확인 방법

```zsh
tail -f logs/collector.log                       # 진행 상황 (50종목마다 기록)
cat data/meta.json                               # last_date, ticker_count, errors
ls data/daily | head -1; ls data/daily | tail -1 # 날짜 범위
dagu history kospi-collector                     # 최근 실행 결과
dagu start kospi-collector                       # 일일 수집 지금 실행
```

Dagu 웹 화면은 맥미니 안(127.0.0.1:8090)에서만 열립니다. 다른 PC에서는 SSH 터널로 엽니다.

```zsh
ssh -N -L 8090:127.0.0.1:8090 afred@ai-macmini   # 그다음 브라우저에서 http://localhost:8090
```

로그인은 `admin`이고, 비밀번호는 맥미니 `~/.config/dagu/admin-password`에 있습니다.

일일 수집은 마지막 수집일부터 오늘까지 받습니다. 마지막 수집일도 다시 받아 잠정치를 확정치로 덮어씁니다.
며칠을 놓쳐도 다음 실행이 빠진 날을 채웁니다. 일봉 실패가 5%를 넘으면 마지막 수집일을 올리지 않아 다음 실행이 다시 받습니다.

로그: `logs/collector.log`(수집기). 시작 실패 같은 실행 기록은 Dagu 화면이나 `dagu history`에서 봅니다.
