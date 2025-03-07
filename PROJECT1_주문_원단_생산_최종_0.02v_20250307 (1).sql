/*
⛏️ 원단정보
*/

WITH CTE AS (
	-- (1) 공정에 제판만 있는 경우
	SELECT NO_SMOR,
		   NO_SEQ,
		   MAX(CD_PROCESS) OVER (PARTITION BY NO_SMOR, NO_SEQ) AS MAX값
	FROM SMOR_ORDER_PRO
	WHERE SUBSTRING(NO_SMOR, 1, 8) BETWEEN '20250226' AND '20250325'),
	 EXCLUDE AS (
		 -- 제외할 항목 정리
		 SELECT DISTINCT NO_SMOR, NO_SEQ
		 FROM CTE
		 WHERE MAX값 = '01'
		 UNION ALL
		 -- (2) 1급지, 2급지가 있는 경우
		 SELECT DISTINCT NO_SMOR, NO_SEQ
		 FROM SMOR_ORDER_MATERIAL
		 -- 😊😊 내일 수정 필요 (특이사항을 어떻게 필터링 할 것인지 생각 필요)
		 WHERE (CHARINDEX('1급지', TX_NOTE) > 0 OR CHARINDEX('2급지', TX_NOTE) > 0)
			OR CHARINDEX('턴키', TX_NOTE) > 0
			 AND SUBSTRING(NO_SMOR, 1, 8) BETWEEN '20250226' AND '20250325'),
	 실제주문 AS (
		 -- 제외된 주문을 제외하고 남은 주문
		 SELECT H.NO_SMOR,
				H.NO_SEQ,
				H.DT_COMP        AS 발주일자,
				H.NM_PRODUCTNAME AS 제품명,
				CONVERT(DATE, H.DT_STDREQ, 112) AS 표준납기일,
				H.QT_AMOUNT      AS 주문수량
		 FROM SMOR_ORDER_H H
				  LEFT JOIN EXCLUDE M ON H.NO_SMOR = M.NO_SMOR AND H.NO_SEQ = M.NO_SEQ
		 WHERE SUBSTRING(H.NO_SMOR, 1, 8) BETWEEN '20250226' AND '20250325'
		   AND M.NO_SMOR IS NULL),
	 최종입고정보 AS (
		 -- 주문번호별 최종 입고일 및 총 입고 수량 계산
		 SELECT NO_SMOR         AS 주문번호,
				NO_SEQ          AS 주문세부번호,
				MAX(DT_INSDATE) AS 최종입고일,
				SUM(QT_ENTER)   AS 최종입고수량
		 FROM MMFP_ENTER_D
		 WHERE SUBSTRING(NO_SMOR, 1, 8) BETWEEN '20250226' AND '20250325'
		 GROUP BY NO_SMOR, NO_SEQ),
	 주문지연여부 AS (SELECT 실제주문.NO_SMOR                                    AS 주문번호,
					   실제주문.NO_SEQ                                     AS 주문세부번호,
					   CONVERT(DATE, 실제주문.발주일자, 112)                   AS 발주일자,
					   실제주문.제품명,
					   CONVERT(DATE, 실제주문.표준납기일, 112)                  AS 표준납기일,
					   실제주문.주문수량,
					   CONVERT(DATE, SUBSTRING(개별입고.DT_INSDATE, 1, 8)) AS 개별입고일자,
					   -- 최종입고일 변환
					   CASE
						   WHEN 최종입고정보.최종입고일 IS NOT NULL
							   THEN CONVERT(DATE, SUBSTRING(최종입고정보.최종입고일, 1, 8))
						   ELSE NULL
						   END                                         AS 최종입고일자,
					   COALESCE(최종입고정보.최종입고수량, 0)                      AS 최종입고수량,

					   -- 표준납기일과 최종입고일자의 일자 차이 계산 (입고 안 됐으면 오늘 날짜로 계산)
					   DATEDIFF(DAY, 실제주문.표준납기일,
									 COALESCE(
											 (CASE
												  WHEN 최종입고정보.최종입고일 IS NOT NULL
													  THEN CONVERT(DATETIME,
																   SUBSTRING(최종입고정보.최종입고일, 1, 4) + '-' +
																   SUBSTRING(최종입고정보.최종입고일, 5, 2) + '-' +
																   SUBSTRING(최종입고정보.최종입고일, 7, 2) + ' ' +
																   SUBSTRING(최종입고정보.최종입고일, 9, 2) + ':' +
																   SUBSTRING(최종입고정보.최종입고일, 11, 2) + ':' +
																   SUBSTRING(최종입고정보.최종입고일, 13, 2))
												  ELSE GETDATE()
												 END), GETDATE()
									 )
					   )                                               AS 입고지연일,

					   -- 입고지연여부: 입고지연일이 1 이상이면 1, 아니면 0
					   CASE
						   WHEN DATEDIFF(DAY, 실제주문.표준납기일,
											  COALESCE(
													  (CASE
														   WHEN 최종입고정보.최종입고일 IS NOT NULL
															   THEN CONVERT(DATETIME,
																			SUBSTRING(최종입고정보.최종입고일, 1, 4) + '-' +
																			SUBSTRING(최종입고정보.최종입고일, 5, 2) + '-' +
																			SUBSTRING(최종입고정보.최종입고일, 7, 2) + ' ' +
																			SUBSTRING(최종입고정보.최종입고일, 9, 2) + ':' +
																			SUBSTRING(최종입고정보.최종입고일, 11, 2) + ':' +
																			SUBSTRING(최종입고정보.최종입고일, 13, 2))
														   ELSE GETDATE()
														  END), GETDATE()
											  )
								) >= 1 THEN 1
						   ELSE 0
						   END                                         AS 주문지연여부

				FROM 실제주문
						 LEFT JOIN MMFP_ENTER_D 개별입고
								   ON 실제주문.NO_SMOR = 개별입고.NO_SMOR
									   AND 실제주문.NO_SEQ = 개별입고.NO_SEQ
						 LEFT JOIN 최종입고정보
								   ON 실제주문.NO_SMOR = 최종입고정보.주문번호
									   AND 실제주문.NO_SEQ = 최종입고정보.주문세부번호),
	 주문지연여부_프로세스 AS (SELECT 주문지연여부.주문번호,
							주문지연여부.주문세부번호,
							주문지연여부.발주일자,
							주문지연여부.제품명,
							주문지연여부.표준납기일,
							주문지연여부.주문수량,
							주문지연여부.최종입고일자,
							주문지연여부.최종입고수량,
							주문지연여부.입고지연일 AS 주문지연일수,
							주문지연여부.주문지연여부,
							SMOR_ORDER_PRO.CD_PROCESS
					 FROM 주문지연여부
							  LEFT JOIN SMOR_ORDER_PRO
										ON 주문지연여부.주문번호 = SMOR_ORDER_PRO.NO_SMOR
											AND 주문지연여부.주문세부번호 = SMOR_ORDER_PRO.NO_SEQ
					 WHERE SUBSTRING(주문지연여부.주문번호, 10, 1) NOT IN ('9', '3')),
	 주문정보FINAL AS (SELECT *
				   FROM 주문지연여부_프로세스 PIVOT (
										   MAX(CD_PROCESS)
							FOR CD_PROCESS IN ([01], [02], [03], [04], [05])
					   ) AS PVT)
		, 원단순서적용1 AS (
SELECT NO_SMOR, NO_SEQ, SOM.CD_SYSITEM, BI.NM_ITEM, CD_ITEMGUBUN, NO_SMORSUB,
	-- ✅ "인쇄(002)"가 존재하는지 확인
	COUNT (CASE WHEN CD_ITEMGUBUN = '002' THEN 1 END)
	OVER (PARTITION BY NO_SMOR, NO_SEQ) AS CNT_002,
	-- ✅ "LLD(PET이지필)"이 포함된 원자재인지 확인
	CASE
	WHEN NM_ITEM LIKE '%LLD(PET이지필)%' THEN 1
	ELSE 0
	END AS IS_LLD,
	-- ✅ 그룹 내 원자재 개수 확인 (002 제외)
	COUNT (*) OVER (PARTITION BY NO_SMOR, NO_SEQ) AS TOTAL_MATERIALS,
	-- ✅ 원자재 순번 부여
	ROW_NUMBER() OVER (PARTITION BY NO_SMOR, NO_SEQ, CD_ITEMGUBUN ORDER BY NO_SMORSUB) AS RN,
	-- ✅ "LLD(PET이지필)"이 아닌 원자재들만 따로 카운트
	ROW_NUMBER() OVER (PARTITION BY NO_SMOR, NO_SEQ, CD_ITEMGUBUN ORDER BY NO_SMORSUB) AS RN_NON_LLD
FROM SMOR_ORDER_MAT1 SOM
	LEFT JOIN BSIT_ITEM BI
ON SOM.CD_SYSITEM = BI.CD_SYSITEM
WHERE CD_ITEMGUBUN <> '003')
	, 원단순서수정1 AS (
SELECT NO_SMOR, NO_SEQ, CD_SYSITEM, NM_ITEM, NO_SMORSUB, CASE
	-- ✅ "LLD(PET이지필)"이 포함된 원자재가 있고, 002가 있으면 → 2
	WHEN IS_LLD = 1 AND CNT_002 > 0 THEN 2
	-- ✅ "LLD(PET이지필)"이 포함된 원자재가 있고, 002가 없는데 유일한 원자재라면 → 1
	WHEN IS_LLD = 1 AND CNT_002 = 0 AND TOTAL_MATERIALS = 1 THEN 1
	-- ✅ "LLD(PET이지필)"이 포함된 원자재가 있고, 002가 없지만 다른 원자재와 함께 있다면 → no_smorsub 순서대로 1, 2, 3, ...
	WHEN IS_LLD = 1 AND CNT_002 = 0 AND TOTAL_MATERIALS > 1 THEN RN
	-- ✅ 일반적인 정렬 로직 적용
	WHEN CD_ITEMGUBUN = '002' THEN 1
	WHEN CD_ITEMGUBUN = '001' AND CNT_002 > 0 THEN RN_NON_LLD + 1
	WHEN CD_ITEMGUBUN = '001' AND CNT_002 = 0 THEN RN_NON_LLD
	END AS ITEM_ORDER_ORG
FROM 원단순서적용1
WHERE SUBSTRING (NO_SMOR
	, 1
	, 8) BETWEEN '20250226'
  AND '20250325')
	, 원단순서적용2 AS (
SELECT NO_SMOR, NO_SEQ, SOM.CD_SYSITEM, BI.NM_ITEM, CD_ITEMGUBUN, NO_SMORSUB,
	-- ✅ "인쇄(002)"가 존재하는지 확인
	COUNT (CASE WHEN CD_ITEMGUBUN = '002' THEN 1 END)
	OVER (PARTITION BY NO_SMOR, NO_SEQ) AS CNT_002,
	-- ✅ "LLD(PET이지필)"이 포함된 원자재인지 확인
	CASE
	WHEN NM_ITEM LIKE '%LLD(PET이지필)%' THEN 1
	ELSE 0
	END AS IS_LLD,
	-- ✅ 그룹 내 원자재 개수 확인 (002 제외)
	COUNT (*) OVER (PARTITION BY NO_SMOR, NO_SEQ) AS TOTAL_MATERIALS,
	-- ✅ 원자재 순번 부여
	ROW_NUMBER() OVER (PARTITION BY NO_SMOR, NO_SEQ, CD_ITEMGUBUN ORDER BY NO_SMORSUB) AS RN,
	-- ✅ "LLD(PET이지필)"이 아닌 원자재들만 따로 카운트
	ROW_NUMBER() OVER (PARTITION BY NO_SMOR, NO_SEQ, CD_ITEMGUBUN ORDER BY NO_SMORSUB) AS RN_NON_LLD
FROM SMOR_ORDER_MAT2 SOM
	LEFT JOIN BSIT_ITEM BI
ON SOM.CD_SYSITEM = BI.CD_SYSITEM
WHERE CD_ITEMGUBUN <> '003')
	, 원단순서수정2 AS (
SELECT NO_SMOR, NO_SEQ, CD_SYSITEM, NM_ITEM, NO_SMORSUB, CASE
	-- ✅ "LLD(PET이지필)"이 포함된 원자재가 있고, 002가 있으면 → 2
	WHEN IS_LLD = 1 AND CNT_002 > 0 THEN 2
	-- ✅ "LLD(PET이지필)"이 포함된 원자재가 있고, 002가 없는데 유일한 원자재라면 → 1
	WHEN IS_LLD = 1 AND CNT_002 = 0 AND TOTAL_MATERIALS = 1 THEN 1
	-- ✅ "LLD(PET이지필)"이 포함된 원자재가 있고, 002가 없지만 다른 원자재와 함께 있다면 → no_smorsub 순서대로 1, 2, 3, ...
	WHEN IS_LLD = 1 AND CNT_002 = 0 AND TOTAL_MATERIALS > 1 THEN RN
	-- ✅ 일반적인 정렬 로직 적용
	WHEN CD_ITEMGUBUN = '002' THEN 1
	WHEN CD_ITEMGUBUN = '001' AND CNT_002 > 0 THEN RN_NON_LLD + 1
	WHEN CD_ITEMGUBUN = '001' AND CNT_002 = 0 THEN RN_NON_LLD
	END AS ITEM_ORDER_ORG
FROM 원단순서적용2
WHERE SUBSTRING (NO_SMOR
	, 1
	, 8) BETWEEN '20250226'
  AND '20250325')
	, 원단정보 AS (
SELECT *
FROM 원단순서수정1
WHERE SUBSTRING (NO_SMOR, CHARINDEX('-', NO_SMOR) + 1, 1) NOT IN ('3', '9')
UNION ALL
SELECT *
FROM 원단순서수정2
WHERE SUBSTRING (NO_SMOR
	, CHARINDEX('-'
	, NO_SMOR) + 1
	, 1) NOT IN ('3'
	, '9'))

-- ✅ 구매발주정보와 결합
-- ✅ NO_SMOR, NO_SEQ, CD_SYSITEM, NO_SMORSUB
	, 구매발주정보결합 AS (
SELECT O.*, DATEFROMPARTS(2025, MONTH (CONVERT (DATE, DT_DUE, 112)), DAY (CONVERT (DATE, DT_DUE, 112))) AS 원단납기일자, BC.NM_CUST AS 매입처명, DATEFROMPARTS(2025, MONTH (CONVERT (DATE, DT_ENTER, 112)), DAY (CONVERT (DATE, DT_ENTER, 112))) AS 원단입고일자, CASE
	WHEN ITEM_ORDER_ORG = 1
	THEN DATEADD(DAY, 2, CONVERT (DATE, SUBSTRING (O.NO_SMOR, 1, 8), 112))
	ELSE DATEADD(DAY, 3, CONVERT (DATE, SUBSTRING (O.NO_SMOR, 1, 8), 112))
	END AS 원단입고계산
FROM 원단정보 AS O
	LEFT JOIN PMOR_ORDER_M POM
ON O.NO_SMOR = POM.NO_SMOR
	AND O.NO_SEQ = POM.NO_SEQ
	AND O.NO_SMORSUB = POM.NO_SMORSUB
	LEFT JOIN BSCT_CUST BC
	ON POM.CD_BUYCUST = BC.CD_CUST
WHERE ITEM_ORDER_ORG IS NOT NULL)
-- ❤️ 주문원단계수 집계
	, 주문원단개수집계 AS (
SELECT NO_SMOR, NO_SEQ, SUM (CASE WHEN ITEM_ORDER_ORG = 1 THEN 1 ELSE 0 END) AS 주문1급지개수, SUM (CASE WHEN ITEM_ORDER_ORG <> 1 THEN 1 ELSE 0 END) AS 주문2급지개수, SUM (CASE WHEN ITEM_ORDER_ORG = 1 AND 원단입고일자 IS NOT NULL THEN 1 ELSE 0 END) AS 원단1급지개수, SUM (CASE WHEN ITEM_ORDER_ORG <> 1 AND 원단입고일자 IS NOT NULL THEN 1 ELSE 0 END) AS 원단2급지개수, MAX (원단납기일자) AS 원단납기일자, MAX (원단입고일자) AS 원단입고일자, MAX (원단입고계산) AS 원단입고계산
FROM 구매발주정보결합
GROUP BY NO_SMOR, NO_SEQ)
		,
	원단정보FINAL AS (
SELECT *, DATEDIFF(DAY, 원단입고일자, 원단입고계산) AS 원단지연일수, CASE
	WHEN DATEDIFF(DAY, 원단입고일자, 원단입고계산) <= 0 THEN 0
	ELSE 1
	END AS 원단지연여부
FROM 주문원단개수집계)
		, 주문_원단결합 AS (
SELECT A.*, 주문1급지개수, 주문2급지개수, 원단1급지개수, 원단2급지개수, 원단납기일자, 원단입고일자, 원단입고계산, 원단지연일수, 원단지연여부
-- b.NO_SMOR
FROM 주문정보FINAL A
	LEFT JOIN 원단정보FINAL B
ON A.주문번호 = B.NO_SMOR
	AND A.주문세부번호 = B.NO_SEQ
WHERE B.NO_SMOR IS NOT NULL)
-- WHERE COALESCE([01], [02], [03], [04], [05]) IS NOT NULL
-- 인쇄, 합지, 분단, 제대 작성
	, 공정별생산실적일 AS (
SELECT A.주문번호, A.주문세부번호, 발주일자, 제품명, 표준납기일, 주문수량, 최종입고일자, 최종입고수량, 주문지연일수, 주문지연여부, 주문1급지개수, 주문2급지개수, 원단1급지개수, 원단2급지개수, 원단납기일자, 원단입고일자, 원단입고계산, 원단지연일수, 원단지연여부, A.[02], A.[03], A.[04], A.[05], MAX (CASE WHEN TEMP.[02] = A.원단1급지개수 THEN CONVERT (DATE, 생산실적일, 112) ELSE NULL END) AS 인쇄생산실적일, MAX (CASE WHEN TEMP.[03] = A.원단2급지개수 THEN CONVERT (DATE, 생산실적일, 112) ELSE NULL END) AS 합지생산실적일, MAX (CASE
	WHEN A.[04] IS NOT NULL AND TEMP.[04] IS NOT NULL THEN CONVERT (DATE, 생산실적일, 112)
	ELSE NULL
	END) AS 분단생산실적일, MAX (CASE
	WHEN A.[05] IS NOT NULL AND TEMP.[05] IS NOT NULL THEN CONVERT (DATE, 생산실적일, 112)
	ELSE NULL
	END) AS 제대생산실적일
FROM 주문_원단결합 AS A
	LEFT JOIN (SELECT *
	FROM (SELECT NO_SMOR, NO_SEQ, CD_PROCESS, MAX (DT_MMOR) AS 생산실적일, COUNT (NO_SMORSUB) AS CNT
	FROM MMOR_WORK_M
	WHERE SUBSTRING (NO_SMOR, 1, 8) BETWEEN '20250226' AND '20250325'
	GROUP BY NO_SMOR, NO_SEQ, CD_PROCESS) AS SOURCETABLE
	PIVOT (
	SUM (CNT) FOR CD_PROCESS IN ([02], [03], [04], [05])
	) AS PIVOTTABLE) AS TEMP
ON TEMP.NO_SMOR = A.주문번호
	AND TEMP.NO_SEQ = A.주문세부번호
GROUP BY A.주문번호, A.주문세부번호, 발주일자, 제품명, 표준납기일, 주문수량,
	최종입고일자, 최종입고수량, 주문지연일수, 주문지연여부,
	주문1급지개수, 주문2급지개수, 원단1급지개수, 원단2급지개수,
	원단납기일자, 원단입고일자, 원단입고계산, 원단지연일수, 원단지연여부, A.[02],
	A.[03],
	A.[04],
	A.[05]),
	CONVERTING AS (
SELECT SMOR_ORDER_MATERIAL.NO_SMOR, SMOR_ORDER_MATERIAL.NO_SEQ, SMOR_ORDER_MATERIAL.NO_SMORSUB, SMOR_ORDER_MATERIAL.CD_ITEMGUBUN, BSIT_ITEM.NM_ITEM,
-- 텐덤 등 이름 이상한거 다 바꿈
	CASE
	WHEN BSIT_ITEM.NM_ITEM LIKE 'DY1(텐덤)%' THEN '텐덤'
	WHEN BSIT_ITEM.NM_ITEM LIKE 'DY3(살균)%' THEN 'DY3'
	WHEN BSIT_ITEM.NM_ITEM LIKE 'DY2(무용제)%' THEN 'DY2'
	WHEN BSIT_ITEM.NM_ITEM LIKE 'TD1(전자)%' THEN 'TD1'
	WHEN BSIT_ITEM.NM_ITEM LIKE 'TD1(텐덤)%' THEN '텐덤'
	WHEN BSIT_ITEM.NM_ITEM LIKE 'TD1(기성)%' THEN 'TD1'
	WHEN BSIT_ITEM.NM_ITEM LIKE 'TD1(매트)%' THEN 'TD1'
	WHEN BSIT_ITEM.NM_ITEM LIKE 'TD1(AC코팅)%' THEN 'TD1'
	WHEN BSIT_ITEM.NM_ITEM LIKE 'TD1(PE)%' THEN 'TD1'
	ELSE BSIT_ITEM.NM_ITEM
	END AS NM_ITEM_MODIFIED
FROM SMOR_ORDER_MATERIAL
	LEFT JOIN BSIT_ITEM
ON SMOR_ORDER_MATERIAL.CD_SYSITEM = BSIT_ITEM.CD_SYSITEM
WHERE SMOR_ORDER_MATERIAL.CD_ITEMGUBUN = '003'
  AND SUBSTRING (SMOR_ORDER_MATERIAL.NO_SMOR
	, 1
	, 8) BETWEEN '20250226'
  AND '20250325')
	, CONVERTING2 AS (
SELECT CONVERTING.*,
-- 합지 생산일수를 합지명에 따라 추가함
	CASE
	WHEN CONVERTING.NM_ITEM_MODIFIED = 'TD1' THEN 3
	WHEN CONVERTING.NM_ITEM_MODIFIED = 'DY2' THEN 2
	WHEN CONVERTING.NM_ITEM_MODIFIED = '텐덤' THEN 5
	WHEN CONVERTING.NM_ITEM_MODIFIED = 'DY2(솔벤트)' THEN 4
	WHEN CONVERTING.NM_ITEM_MODIFIED = 'DY1' THEN 3
	WHEN CONVERTING.NM_ITEM_MODIFIED = 'DY3' THEN 6
	WHEN CONVERTING.NM_ITEM_MODIFIED = 'TD2' THEN 3
	ELSE NULL
	END AS 합지생산일수
FROM CONVERTING), CONVERTING3 AS (
-- 합지 방법별 평균 합지일수 구한 다음에 총 합계 합지생산일수를 주문번호 따라 생성
SELECT CONVERTING2.NO_SMOR, CONVERTING2.NM_ITEM_MODIFIED, AVG (CONVERTING2.합지생산일수) AS 평균_합지생산일수
FROM CONVERTING2
GROUP BY CONVERTING2.NO_SMOR, CONVERTING2.NM_ITEM_MODIFIED),
	CONVERTING4 AS (
SELECT CONVERTING3.NO_SMOR, SUM (CONVERTING3.평균_합지생산일수) AS 합계_합지생산일수
FROM CONVERTING3
GROUP BY CONVERTING3.NO_SMOR),
	공정별계산일수 AS (
SELECT 공정별생산실적일.*, CONVERTING4.합계_합지생산일수

-- 계산된 인쇄, 합지, 분단, 제대 표준납기일 계산
-- 이후, 차이를 구해서 지연여부 파악
FROM 공정별생산실적일
	LEFT JOIN CONVERTING4
ON 공정별생산실적일.주문번호 = CONVERTING4.NO_SMOR)
	,
	공정별입고계산 AS (
SELECT 공정별계산일수.*,
-- 인쇄 공정별 계산일수
	CASE
	WHEN 공정별계산일수.[02] IS NOT NULL THEN DATEADD(Day, 3, 원단입고계산)
	WHEN 공정별계산일수.[02] IS NULL THEN NULL
	ELSE NULL
	END AS 인쇄입고계산, CASE
	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NOT NULL
	THEN DATEADD(Day, 3 + 합계_합지생산일수, 원단입고계산)
	WHEN [02] IS NOT NULL AND [03] IS NULL THEN DATEADD(Day, 3 + 합계_합지생산일수, 원단입고계산)
	WHEN [02] IS NULL AND [03] IS NOT NULL THEN DATEADD(DAY, 합계_합지생산일수, 원단입고계산)
	WHEN [02] IS NULL AND [03] IS NULL THEN 원단입고계산
	ELSE NULL
	END AS 합지입고계산,

-- 분단 공정별 계산일수
	CASE
	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NOT NULL
	AND 공정별계산일수.[04] IS NOT NULL THEN DATEADD(DAY, 4 + 공정별계산일수.합계_합지생산일수, 원단입고계산)
	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NOT NULL
	AND 공정별계산일수.[04] IS NULL THEN NULL
	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NULL
	AND 공정별계산일수.[04] IS NOT NULL THEN DATEADD(DAY, 4, 원단입고계산)
	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NULL
	AND 공정별계산일수.[04] IS NULL THEN NULL
--
-- 인쇄 O & 합지 X

	WHEN 공정별계산일수.[02] IS NULL AND 공정별계산일수.[03] IS NOT NULL
	AND 공정별계산일수.[04] IS NOT NULL THEN DATEADD(DAY, 1 + 합계_합지생산일수, 원단입고계산)
	WHEN 공정별계산일수.[02] IS NULL AND 공정별계산일수.[03] IS NOT NULL
	AND 공정별계산일수.[04] IS NULL THEN NULL
	WHEN 공정별계산일수.[02] IS NULL AND 공정별계산일수.[03] IS NULL
	AND 공정별계산일수.[04] IS NOT NULL THEN DATEADD(DAY, 1, 원단입고계산)
	WHEN 공정별계산일수.[02] IS NULL AND 공정별계산일수.[03] IS NULL
	AND 공정별계산일수.[04] IS NULL THEN NULL
	ELSE NULL
	END AS 분단입고계산,


-- 제대 공정별 계산일수
-- 인쇄 O & 합지 O
	CASE
	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NOT NULL
	AND 공정별계산일수.[04] IS NOT NULL AND 공정별계산일수.[05] IS NOT NULL
	THEN DATEADD(DAY, 7 + 공정별계산일수.합계_합지생산일수, 원단입고계산)
	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NOT NULL
	AND 공정별계산일수.[04] IS NOT NULL AND 공정별계산일수.[05] IS NULL
	THEN NULL
	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NOT NULL
	AND 공정별계산일수.[04] IS NULL AND 공정별계산일수.[05] IS NOT NULL
	THEN DATEADD(DAY, 6 + 공정별계산일수.합계_합지생산일수, 원단입고계산)
	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NOT NULL
	AND 공정별계산일수.[04] IS NULL AND 공정별계산일수.[05] IS NULL
	THEN NULL

-- 인쇄 O & 합지 X

	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NULL
	AND 공정별계산일수.[04] IS NOT NULL AND 공정별계산일수.[05] IS NOT NULL THEN DATEADD(DAY, 7, 원단입고계산)
	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NULL
	AND 공정별계산일수.[04] IS NOT NULL AND 공정별계산일수.[05] IS NULL THEN NULL
	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NULL
	AND 공정별계산일수.[04] IS NULL AND 공정별계산일수.[05] IS NOT NULL THEN DATEADD(DAY, 6, 원단입고계산)
	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NULL
	AND 공정별계산일수.[04] IS NULL AND 공정별계산일수.[05] IS NULL THEN NULL

-- 인쇄 X & 합지 O
	WHEN 공정별계산일수.[02] IS NULL AND 공정별계산일수.[03] IS NOT NULL
	AND 공정별계산일수.[04] IS NOT NULL AND 공정별계산일수.[05] IS NOT NULL
	THEN DATEADD(DAY, 4 + 공정별계산일수.합계_합지생산일수, 원단입고계산)
	WHEN 공정별계산일수.[02] IS NULL AND 공정별계산일수.[03] IS NOT NULL
	AND 공정별계산일수.[04] IS NOT NULL AND 공정별계산일수.[05] IS NULL
	THEN NULL
	WHEN 공정별계산일수.[02] IS NULL AND 공정별계산일수.[03] IS NOT NULL
	AND 공정별계산일수.[04] IS NULL AND 공정별계산일수.[05] IS NOT NULL
	THEN DATEADD(DAY, 3 + 공정별계산일수.합계_합지생산일수, 원단입고계산)
	WHEN 공정별계산일수.[02] IS NULL AND 공정별계산일수.[03] IS NOT NULL
	AND 공정별계산일수.[04] IS NULL AND 공정별계산일수.[05] IS NULL
	THEN NULL

-- 인쇄 X & 합지 X

	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NULL
	AND 공정별계산일수.[04] IS NOT NULL AND 공정별계산일수.[05] IS NOT NULL THEN DATEADD(DAY, 4, 원단입고계산)
	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NULL
	AND 공정별계산일수.[04] IS NOT NULL AND 공정별계산일수.[05] IS NULL THEN NULL
	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NULL
	AND 공정별계산일수.[04] IS NULL AND 공정별계산일수.[05] IS NOT NULL THEN DATEADD(DAY, 3, 원단입고계산)
	WHEN 공정별계산일수.[02] IS NOT NULL AND 공정별계산일수.[03] IS NULL
	AND 공정별계산일수.[04] IS NULL AND 공정별계산일수.[05] IS NULL THEN NULL
	ELSE NULL
	END AS 제대입고계산
FROM 공정별계산일수), temp as (
SELECT *, DATEDIFF(DAY, 인쇄생산실적일, 인쇄입고계산) AS 인쇄지연일수, DATEDIFF(DAY, 합지생산실적일, 합지입고계산) AS 합지지연일수, DATEDIFF(DAY, 분단생산실적일, 분단입고계산) AS 분단지연일수, DATEDIFF(DAY, 제대생산실적일, 제대입고계산) AS 제대지연일수
FROM 공정별입고계산)
		, 지연여부final AS (
SELECT *,
	-- 인쇄를 하는 경우 (temp.[02] IS NOT NULL)
	CASE
	WHEN temp.[02] IS NOT NULL THEN
	CASE
	-- 인쇄 생산실적일이 있는 경우
	WHEN temp.인쇄생산실적일 IS NOT NULL THEN
	CASE WHEN temp.인쇄지연일수 <= 0 THEN 0 ELSE 1 END
	-- 인쇄 생산실적일이 없는 경우
	ELSE
	CASE
	WHEN temp.인쇄입고계산 IS NOT NULL AND DATEDIFF(DAY, GETDATE(), temp.인쇄입고계산) <= 0 THEN 1
	ELSE 0
	END
	END
	-- 인쇄를 하지 않는 경우
	ELSE NULL
	END AS 인쇄지연여부,
	-- 합지를 하는 경우
	CASE
	WHEN temp.[03] IS NOT NULL THEN
	CASE
	-- 합지 생산실적일이 있는 경우
	WHEN temp.합지생산실적일 IS NOT NULL THEN
	CASE WHEN temp.합지지연일수 <= 0 THEN 0 ELSE 1 END
	-- 합지 생산실적일이 없는 경우
	ELSE
	CASE
	WHEN temp.합지입고계산 IS NOT NULL AND DATEDIFF(DAY, GETDATE(), temp.합지입고계산) <= 0 THEN 1
	ELSE 0
	END
	END
	-- 합지를 하지 않는 경우
	ELSE NULL
	END AS 합지지연여부, CASE
	WHEN temp.[04] IS NOT NULL THEN
	CASE
	-- 분단 생산실적일이 있는 경우
	WHEN temp.분단생산실적일 IS NOT NULL THEN
	CASE WHEN temp.분단지연일수 <= 0 THEN 0 ELSE 1 END
	-- 분단 생산실적일이 없는 경우
	ELSE
	CASE
	WHEN temp.분단입고계산 IS NOT NULL AND DATEDIFF(DAY, GETDATE(), temp.분단입고계산) <= 0 THEN 1
	ELSE 0
	END
	END
	-- 분단을 하지 않는 경우
	ELSE NULL
	END AS 분단지연여부, CASE
	WHEN temp.[05] IS NOT NULL THEN
	CASE
	-- 제대 생산실적일이 있는 경우
	WHEN temp.제대생산실적일 IS NOT NULL THEN
	CASE WHEN temp.제대지연일수 <= 0 THEN 0 ELSE 1 END
	-- 제대 생산실적일이 없는 경우
	ELSE
	CASE
	WHEN temp.제대입고계산 IS NOT NULL AND DATEDIFF(DAY, GETDATE(), temp.제대입고계산) <= 0 THEN 1
	ELSE 0
	END
	END
	-- 제대를 하지 않는 경우
	ELSE NULL
	END AS 제대지연여부
FROM temp
	)
, 고객사 AS (SELECT a.*,
				 BC.NM_CUST AS 고객사
		  FROM 지연여부final a
				   LEFT JOIN SMOR_ORDER_H b
							 ON a.주문번호 = b.NO_SMOR
								 AND a.주문세부번호 = b.NO_SEQ
				   LEFT JOIN BSCT_CUST BC
							 ON BC.CD_CUST = b.CD_CUST)
, abc AS (SELECT DISTINCT SOP.NO_SMOR,
							 SOP.NO_SEQ,
							 SOP.CD_PROCESS,
							 MP.NM_PROCESS,
							 BC.NM_CUST AS 외주처명
			 FROM SMOR_ORDER_PRO SOP
					  LEFT JOIN BSCT_CUST BC
								ON SOP.CD_CUST = BC.CD_CUST
					  LEFT JOIN MMIT_PROCESS MP
								ON MP.CD_PROCESS = SOP.CD_PROCESS

			 WHERE SUBSTRING(NO_SMOR, 1, 8) BETWEEN '20250226' AND '20250325')

,
real_final AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY NO_SMOR, NO_SEQ, CD_PROCESS ORDER BY CD_PROCESS) AS rn
    FROM abc
),
PIVOT_DATA AS (
    -- 중복된 NM_PROCESS별 데이터가 없도록 GROUP BY 수행
    SELECT NO_SMOR,
           NO_SEQ,
           MAX(CASE WHEN NM_PROCESS = '인쇄' THEN 외주처명 END) AS 인쇄,
           MAX(CASE WHEN NM_PROCESS = '합지' THEN 외주처명 END) AS 합지,
           MAX(CASE WHEN NM_PROCESS = '분단' THEN 외주처명 END) AS 분단,
           MAX(CASE WHEN NM_PROCESS = '제대' THEN 외주처명 END) AS 제대
    FROM real_final
    GROUP BY NO_SMOR, NO_SEQ
)

SELECT
	a.*, p.인쇄, p.합지, p.분단, p.제대
FROM 고객사 a
	LEFT JOIN PIVOT_DATA p
		ON a.주문번호 = p.NO_SMOR
		AND a.주문세부번호 = p.NO_SEQ

