# MSA 분산 트랜잭션과 이벤트 처리 패턴

> 학습 범위: Saga Pattern, Transactional Outbox Pattern, Spring Kafka `@KafkaListener`, Debezium SMT/Envelope, DLT/DLQ  
> 목적: 기술 학습 및 팀 발표용  
> 기준: 개념과 처리 흐름 중심. 예제 코드는 동작 원리를 설명하기 위한 예시이며 실제 설정은 사용하는 Spring Kafka·Kafka·Debezium 버전에 맞춰 조정한다.

## 목차

1. 전체 관계 이해
2. Saga Pattern
3. Transactional Outbox Pattern
4. Debezium CDC와 Envelope
5. SMT(Single Message Transform)
6. Spring Kafka `@KafkaListener`
7. DLT와 DLQ
8. 전체 흐름 예시
9. 핵심 비교 및 학습 체크리스트

---

## 1. 전체 관계 이해

이 기술들은 같은 문제를 해결하는 대체재가 아니다. 각자 담당하는 범위가 다르며, 하나의 MSA 이벤트 처리 구조에서 함께 사용할 수 있다.

| 기술 | 해결하는 문제 | 핵심 역할 |
|---|---|---|
| Saga | 여러 서비스에 걸친 업무 트랜잭션 | 단계별 로컬 트랜잭션과 보상 작업 관리 |
| Transactional Outbox | DB 저장과 메시지 발행의 이중 쓰기 문제 | 업무 데이터와 이벤트를 같은 DB 트랜잭션으로 저장 |
| Debezium CDC | DB 변경 사항을 외부로 전달 | DB 로그를 읽어 변경 이벤트 생성 |
| SMT | Connect 메시지 구조와 내용을 변환 | 메시지의 key/value/header 등을 변환 |
| Kafka | 서비스 간 이벤트 전달 | Topic에 이벤트를 저장하고 Consumer에 전달 |
| `@KafkaListener` | Spring 애플리케이션의 Kafka 소비 | 지정 Topic의 레코드를 받아 메서드 실행 |
| DLT/DLQ | 반복 실패·처리 불가 메시지 격리 | 실패 메시지를 별도 Topic/Queue로 보내 추후 분석·재처리 |

핵심 구분:
- Saga는 **업무 흐름과 분산 트랜잭션**을 관리한다.
- Outbox는 **DB 변경과 이벤트 발행 사이의 유실 위험**을 줄인다.
- Debezium은 **DB 변경 로그를 읽어 이벤트로 전달**한다.
- SMT는 **Kafka Connect를 통과하는 레코드를 변환**한다.
- `@KafkaListener`는 **Spring Kafka Consumer의 메시지 수신 진입점**이다.
- DLT/DLQ는 **정상 처리하지 못한 메시지를 격리**한다.

## 2. Saga Pattern

### 2.1 정의와 필요한 이유

Saga는 여러 마이크로서비스에 걸친 하나의 비즈니스 트랜잭션을 여러 개의 독립적인 로컬 트랜잭션으로 나누어 실행하고, 중간 단계가 실패하면 앞서 성공한 작업을 보상 트랜잭션으로 상쇄하는 패턴이다.

MSA에서는 각 서비스가 자기 데이터베이스를 소유하는 경우가 많다. 따라서 주문 DB, 결제 DB, 재고 DB를 하나의 일반적인 로컬 DB 트랜잭션으로 묶어 원자적으로 커밋하거나 롤백하기 어렵다.

예시 주문 흐름:
1. 주문 서비스: 주문 생성
2. 결제 서비스: 결제 승인
3. 재고 서비스: 재고 예약
4. 배송 서비스: 배송 요청 생성

배송 요청 단계가 실패했다고 해서 앞선 서비스의 DB 트랜잭션을 물리적으로 되돌릴 수는 없다. Saga는 결제 취소, 재고 예약 해제, 주문 취소와 같은 별도의 업무 작업을 실행한다.

### 2.2 보상 트랜잭션(Compensating Transaction)

보상 트랜잭션은 이미 커밋된 로컬 트랜잭션의 결과를 업무적으로 취소하거나 상쇄하는 새로운 트랜잭션이다.

| 정방향 작업 | 보상 작업 예시 |
|---|---|
| 주문 생성 | 주문 취소 처리 |
| 결제 승인 | 결제 취소 또는 환불 |
| 재고 예약 | 재고 예약 해제 |
| 배송 요청 생성 | 배송 요청 취소 |

보상은 DB의 `ROLLBACK`과 다르다. 이미 외부 결제 시스템에 전달된 승인 요청을 과거로 되돌리는 것이 아니라, 취소·환불이라는 새로운 업무 명령을 수행한다. 보상 자체도 실패할 수 있으므로 재시도, 보상 상태 관리, 운영자 개입 절차가 필요하다.

### 2.3 두 가지 구현 방식

#### A. Choreography(코레오그래피)

중앙의 진행 관리자가 없이 각 서비스가 이벤트를 구독하고, 자기 작업을 수행한 뒤 다음 이벤트를 발행한다.

예:
`OrderCreated` → 결제 서비스 → `PaymentCompleted` → 재고 서비스 → `InventoryReserved` → 배송 서비스

장점:
- 중앙 오케스트레이터가 필요하지 않다.
- 서비스 간 직접적인 제어 결합을 줄일 수 있다.
- 단순하고 짧은 이벤트 흐름에 적합하다.

단점:
- 서비스가 늘면 전체 흐름을 한눈에 파악하기 어렵다.
- 이벤트 간 의존 관계와 장애 원인 추적이 복잡해질 수 있다.
- 순환 이벤트나 중복 처리 방지 설계가 중요하다.

#### B. Orchestration(오케스트레이션)

중앙 Saga 오케스트레이터가 각 서비스에 명령을 보내고 결과를 받아 다음 단계를 결정한다.

장점:
- 전체 업무 흐름과 현재 상태를 중앙에서 파악하기 쉽다.
- 실패 시 어떤 보상 작업을 수행할지 명시적으로 관리할 수 있다.
- 단계가 많고 분기·시간 제한·보상 규칙이 복잡한 프로세스에 적합하다.

단점:
- 오케스트레이터와 Saga 상태 저장·복구 로직이 필요하다.
- 오케스트레이터가 비즈니스 로직을 과도하게 떠안지 않도록 경계를 설계해야 한다.
- 고가용성, 재시작 후 복구, 중복 명령 처리가 필요하다.

### 2.4 Saga의 필수 고려 사항

- **최종적 일관성:** 모든 서비스가 동시에 같은 순간에 변경되는 구조가 아니다. 처리 중에는 `PENDING` 등 중간 상태가 존재할 수 있다.
- **멱등성:** 동일 명령이나 이벤트가 재전달되어도 업무 결과가 중복 적용되지 않아야 한다.
- **재시도와 타임아웃:** 일시적 오류와 영구적 오류를 구분하고 재시도 정책을 둔다.
- **보상 실패:** 보상도 실패할 수 있으므로 별도 상태, 재시도, 운영자 처리 경로가 필요하다.
- **동시성:** 재고·쿠폰처럼 공유 자원을 여러 Saga가 동시에 변경할 때 조건부 갱신이나 예약 모델 등을 고려한다.
- **추적성:** `sagaId`, `correlationId`, `causationId` 등을 통해 단계별 이벤트를 연결한다.

Saga는 전체 시스템에 대한 전통적인 ACID 원자성을 제공하지 않는다. 각 서비스의 로컬 트랜잭션은 ACID로 처리하되, 서비스 간 일관성은 보상과 최종적 일관성으로 맞춘다.

---

## 3. Transactional Outbox Pattern

### 3.1 정의

Transactional Outbox는 서비스의 비즈니스 데이터 변경과 발행할 이벤트를 **동일한 데이터베이스 트랜잭션**으로 저장한 뒤, 별도 릴레이가 Outbox 이벤트를 메시지 브로커에 전달하는 패턴이다.

### 3.2 이중 쓰기 문제(Dual Write Problem)

다음과 같이 DB와 Kafka에 따로 쓰는 코드는 실패 조합을 만든다.

```java
@Transactional
public void createOrder(Order order) {
    orderRepository.save(order);
    kafkaTemplate.send("order-events", toEvent(order));
}
```

가능한 문제:
1. DB 커밋은 성공했지만 Kafka 발행이 실패한다. 주문은 존재하지만 다른 서비스가 주문 생성 사실을 모른다.
2. Kafka 발행은 성공했지만 DB 트랜잭션이 롤백된다. 다른 서비스는 존재하지 않는 주문을 처리할 수 있다.

Spring의 `@Transactional`은 기본적으로 해당 DB 트랜잭션을 관리한다. 별도 설정이나 분산 트랜잭션 프로토콜 없이 DB와 Kafka의 쓰기가 자동으로 하나의 원자적 트랜잭션이 되는 것은 아니다.

### 3.3 Outbox 방식

한 DB 트랜잭션 안에서 다음 두 작업을 함께 수행한다.
- 주문 테이블에 주문 저장
- Outbox 테이블에 `OrderCreated` 이벤트 저장

둘 다 커밋되거나 둘 다 롤백된다. 이후 릴레이가 Outbox 이벤트를 Kafka에 전달한다.

```text
주문 요청
   |
   v
[하나의 DB 트랜잭션]
  ├─ orders 테이블 INSERT
  └─ outbox_event 테이블 INSERT
   |
 COMMIT
   |
   v
Outbox Relay
   |
   v
Kafka Topic
   |
   v
각 서비스 Consumer
```

### 3.4 Outbox 테이블 예시

```sql
CREATE TABLE outbox_event (
    id             VARCHAR(100) PRIMARY KEY,
    aggregatetype  VARCHAR(100) NOT NULL,
    aggregateid    VARCHAR(100) NOT NULL,
    type           VARCHAR(100) NOT NULL,
    payload        JSON NOT NULL,
    created_at     TIMESTAMP NOT NULL
);
```

| 컬럼 | 의미 |
|---|---|
| `id` | 이벤트 고유 ID |
| `aggregatetype` | 이벤트가 속한 집합/도메인 유형(예: Order) |
| `aggregateid` | 해당 도메인 객체의 ID(예: 주문번호) |
| `type` | 이벤트 유형(예: OrderCreated) |
| `payload` | 이벤트 본문 |
| `created_at` | 이벤트 생성 시각 |

실제 Debezium Outbox Event Router의 기본 필드 매핑과 맞추려면 컬럼명·타입·설정을 함께 확인해야 한다. 위 테이블은 이해를 위한 예시다.

### 3.5 메시지 릴레이 두 방식

**Polling Publisher**
- 애플리케이션 또는 별도 릴레이가 주기적으로 Outbox 테이블을 조회한다.
- 미발행 이벤트를 Kafka에 전송하고 발행 상태를 갱신하거나 처리 레코드를 관리한다.
- 구현이 직관적이지만 폴링 부하, 조회 지연, 여러 릴레이 인스턴스 간 잠금·경쟁, 발행 후 상태 갱신 전 장애에 따른 중복 발행을 고려해야 한다.

**CDC 방식**
- Debezium Connector가 DB 트랜잭션 로그를 읽는다.
- Outbox 테이블의 INSERT 변경을 감지해 Kafka Connect 레코드로 만든다.
- Outbox Event Router SMT를 적용해 이벤트를 목적 Topic과 원하는 메시지 형태로 라우팅할 수 있다.
- DB 종류별 커넥터 설정, 로그 보존, 오프셋, 스키마, 커넥터 운영을 관리해야 한다.

### 3.6 Outbox가 보장하지 않는 것

Outbox는 DB와 이벤트 기록을 원자적으로 저장해 발행 누락 위험을 줄이는 패턴이지, 모든 상황에서 중복 없는 Exactly-Once 처리를 보장하는 만능 기능은 아니다.

- 릴레이 재시작이나 재시도 과정에서 동일 이벤트가 다시 전달될 수 있다.
- Consumer는 이벤트 ID를 이용해 중복 처리 여부를 확인하는 등 멱등성을 구현해야 한다.
- 이벤트 순서가 중요하면 aggregate ID를 Kafka key로 사용하는 방식, 파티션, DB 로그 순서, 병렬 처리 정책을 함께 설계한다.
- 발행된 Outbox 레코드의 보관·삭제·아카이빙 정책이 필요하다.

---

## 4. Debezium CDC와 Envelope

### 4.1 CDC란?

CDC(Change Data Capture)는 데이터베이스에서 발생한 데이터 변경을 감지하고 변경 내용을 다른 시스템으로 전달하는 기술이다.

Debezium은 여러 데이터베이스의 변경 로그를 읽어 변경 이벤트를 Kafka Connect를 통해 Kafka로 전달하는 CDC 플랫폼이다.

예를 들어 DB에서 주문 행이 INSERT되면 Debezium은 그 변경을 나타내는 이벤트 레코드를 만든다.

### 4.2 Debezium 이벤트의 Envelope 구조

여기서 말하는 **Envelope(엔벌로프, 봉투)** 는 Debezium 변경 이벤트를 감싸는 표준적인 구조를 뜻한다. 단순히 DB의 row 자체만 전달하는 것이 아니라, 변경 데이터와 변경 맥락을 함께 담는다.

일반적인 Debezium 변경 이벤트의 value는 다음과 같은 구조를 가진다.

```json
{
  "before": null,
  "after": {
    "id": 101,
    "status": "CREATED",
    "amount": 35000
  },
  "source": {
    "db": "commerce",
    "table": "orders"
  },
  "op": "c",
  "ts_ms": 1793760000000
}
```

주요 필드:
- `before`: 변경 전 행(row). INSERT에서는 보통 `null`.
- `after`: 변경 후 행(row). INSERT에서는 새 행의 값.
- `source`: 변경이 발생한 DB·테이블·로그 위치 등 출처 메타데이터.
- `op`: 변경 유형. 대표적으로 `c`(create/insert), `u`(update), `d`(delete), `r`(snapshot read).
- `ts_ms`: 이벤트 시간 관련 타임스탬프.

실제 필드와 타입은 DB 커넥터, Debezium 버전, 스키마 및 설정에 따라 달라질 수 있다. 위 JSON은 구조를 이해하기 위한 단순화 예시다.

**이해 포인트**
- Row: 테이블의 한 행에 해당하는 실제 데이터.
- Envelope: 그 row와 함께 변경 전·후 값, 출처, 변경 유형 등의 정보를 감싼 이벤트 구조.
- 따라서 Envelope 전체와 `after` 안의 row는 같은 것이 아니다.

### 4.3 Unwrap(언랩)과의 관계

수업에서 들은 “인밸오프”는 문맥상 **Envelope** 또는 **Unwrap**을 가리켰을 가능성이 있다. 두 용어는 구분해야 한다.

- **Envelope:** Debezium 변경 이벤트를 감싸는 구조.
- **Unwrap:** 그 Envelope에서 실제 row 데이터(대개 `after`)를 꺼내 평탄화하는 변환 작업.

Debezium의 `ExtractNewRecordState` SMT가 대표적인 이벤트 unwrap 변환이다.

변환 전:
```json
{
  "before": null,
  "after": {
    "id": 101,
    "status": "CREATED"
  },
  "source": { "table": "orders" },
  "op": "c"
}
```

변환 후(개념 예시):
```json
{
  "id": 101,
  "status": "CREATED"
}
```

즉, Envelope 전체가 전달되던 메시지에서 `after`의 row 중심으로 메시지를 단순화할 수 있다. 다만 삭제 이벤트는 `after`가 `null`이므로 삭제 이벤트 처리 옵션을 별도로 고려해야 한다. 메타데이터를 헤더나 별도 필드로 보존할지도 결정해야 한다.

**중요한 정정:** Unwrap이 “row를 메모리에 담는 것” 자체를 뜻하는 것은 아니다. SMT는 Kafka Connect가 처리하는 각 SourceRecord/SinkRecord의 key, value, header 등을 변환하는 기능이다. 변환 과정에서 레코드 객체가 처리되지만, 핵심 개념은 메모리 저장이 아니라 **레코드 구조 변환**이다.

### 4.4 “row를 자신만의 구성으로 바꿔 메모리에 담는다”는 설명 해석

강의에서 들은 표현은 다음과 같은 내부 처리 과정을 가리켰을 가능성이 있다.

1. Debezium Connector가 DB 로그에서 변경 사항을 읽는다.
2. 변경 내용을 Kafka Connect의 레코드(SourceRecord) 구조로 만든다.
3. 레코드의 value에는 Debezium이 정의한 Envelope 구조가 들어갈 수 있다. 이 구조에는 row 데이터뿐 아니라 변경 메타데이터도 포함된다.
4. 설정된 SMT가 레코드를 받아 새로운 key/value/topic/header 구조로 변환한다.
5. 변환된 레코드가 Kafka Connect 파이프라인을 거쳐 Kafka로 전달된다.

여기서 “메모리에 담는다”는 말은 처리 중인 Connector/Connect 프로세스의 메모리에서 레코드 객체를 구성·변환한다는 뜻으로 이해할 수 있다. DB에 별도의 테이블로 저장한다거나, Consumer 애플리케이션의 메모리에 영구 보관한다는 뜻은 아니다.

또한 **Outbox Event Router가 만드는 이벤트 형식**과 **ExtractNewRecordState가 만드는 평탄화 형식**은 다르다.
- Outbox Event Router는 Outbox 행을 도메인 이벤트로 구성한다. 기본 설정에서는 Kafka 메시지 value가 payload 자체가 될 수 있다.
- 추가 컬럼을 `envelope` 위치로 배치하도록 설정하면 payload와 추가 필드를 함께 감싸는 Envelope 형태가 만들어질 수 있다.
- ExtractNewRecordState는 일반 Debezium 변경 이벤트의 Envelope에서 `after` row를 추출해 평탄화한다.

따라서 강의의 표현만으로 어떤 SMT를 말하는지 단정할 수는 없다. **Outbox 테이블 row를 도메인 이벤트로 재구성하는 설명이라면 Outbox Event Router**, 일반 CDC 이벤트의 `before/after/source/op` 중 `after`만 꺼내는 설명이라면 **ExtractNewRecordState(Unwrap)** 를 말하는 것이다.

---

## 5. SMT(Single Message Transform)

### 5.1 SMT의 정의

SMT는 Kafka Connect에서 개별 메시지 레코드를 가볍게 변환하는 기능이다. Connector가 읽거나 쓰는 레코드에 대해 key, value, header, topic 등을 변경할 수 있다.

SMT는 Kafka Connect의 변환 단계이며, 일반적인 Kafka Consumer 애플리케이션의 메시지 처리 로직과는 구분된다.

### 5.2 SMT가 변환할 수 있는 것

- 레코드의 key 또는 value 추출·변환
- 필드 추가, 삭제, 이름 변경
- 특정 필드 마스킹 또는 제거
- Topic 이름 변경 또는 라우팅
- 헤더 추가·수정
- Debezium Envelope에서 새 row 추출

SMT는 단순하고 레코드 단위로 수행할 변환에 적합하다. 복잡한 조인, 여러 레코드에 걸친 상태 처리, 비즈니스 워크플로는 보통 Consumer 애플리케이션이나 별도 스트림 처리 시스템의 책임이다.

### 5.3 Outbox Event Router SMT

Transactional Outbox와 Debezium을 함께 쓸 때 특히 중요한 SMT가 **Outbox Event Router**다.

Outbox 테이블의 행을 일반적인 DB 변경 이벤트 그대로 전달하는 대신, Outbox 행의 컬럼을 해석해 도메인 이벤트 형태로 라우팅한다.

예를 들어 Outbox 행에 다음 값이 있다고 하자.

| 컬럼 | 값 |
|---|---|
| `id` | `evt-001` |
| `aggregatetype` | `Order` |
| `aggregateid` | `ORD-1001` |
| `type` | `OrderCreated` |
| `payload` | 주문 이벤트 JSON |

Event Router는 설정에 따라 다음을 수행할 수 있다.
- `aggregatetype`과 설정된 라우팅 규칙을 이용해 Topic을 결정한다.
- `aggregateid`를 메시지 key로 설정해 같은 aggregate의 이벤트가 같은 파티션으로 갈 수 있도록 한다.
- `type`을 이벤트 유형 정보로 활용한다.
- `payload`를 메시지 본문으로 전달한다.
- 이벤트 ID 등 추가 컬럼을 헤더에 넣도록 설정할 수 있다.

개념적인 결과:
```text
Topic: Order
Key: ORD-1001
Value: { "orderId": "ORD-1001", "status": "CREATED" }
Header/metadata: event type, event id 등(설정에 따름)
```

실제 Topic 이름, key, payload, 헤더 매핑은 Outbox Event Router 설정에 따라 달라진다.

### 5.4 ExtractNewRecordState와 Outbox Event Router는 다르다

| 구분 | ExtractNewRecordState | Outbox Event Router |
|---|---|---|
| 주 용도 | Debezium Envelope에서 새 row 추출 | Outbox 행을 도메인 이벤트로 라우팅 |
| 입력 | 일반 테이블의 Debezium 변경 이벤트 | Outbox 테이블 변경 이벤트 |
| 핵심 처리 | `after` 중심으로 unwrap | Outbox 컬럼 해석, Topic/key/payload 구성 |
| 대표 사용처 | CDC 변경 이벤트를 단순한 row 형태로 전달 | Transactional Outbox 구현 |

둘 다 SMT이지만 목적이 다르다. Outbox 패턴을 구현할 때는 Outbox Event Router를 중심으로 이해해야 하며, 일반 테이블 변경 이벤트의 Envelope를 평탄화하는 ExtractNewRecordState와 혼동하지 않아야 한다.

---

## 6. Spring Kafka `@KafkaListener`

### 6.1 정의

`@KafkaListener`는 Spring for Apache Kafka에서 제공하는 어노테이션으로, 지정한 Kafka Topic의 메시지를 수신할 메서드를 선언할 때 사용한다.

쉽게 말해 **Spring 애플리케이션에서 Kafka Consumer의 메시지 수신 지점을 표시하는 어노테이션**이다.

Debezium이 Kafka에 이벤트를 넣는 쪽이라면, `@KafkaListener`는 Spring 서비스가 Kafka에서 이벤트를 꺼내 처리하는 쪽에서 사용된다.

### 6.2 기본 예시

```java
@Component
public class OrderEventConsumer {

    @KafkaListener(
        topics = "order-events",
        groupId = "inventory-service"
    )
    public void consume(String message) {
        System.out.println("수신 메시지: " + message);
    }
}
```

동작 개념:
1. Spring Kafka가 Listener Container를 구성한다.
2. Container 내부의 Kafka Consumer가 Topic을 구독한다.
3. Consumer Group 규칙에 따라 파티션을 할당받는다.
4. 레코드를 가져오면 Listener 메서드를 호출한다.
5. 메서드의 인자 타입과 설정에 따라 String, DTO, ConsumerRecord 등으로 전달받는다.

### 6.3 자주 보는 인자 형태

```java
@KafkaListener(topics = "order-events", groupId = "inventory-service")
public void consume(OrderCreatedEvent event) {
    // 역직렬화된 이벤트 객체를 처리
}
```

메시지 메타데이터도 필요하다면 `ConsumerRecord` 등을 인자로 받을 수 있다.

```java
@KafkaListener(topics = "order-events", groupId = "inventory-service")
public void consume(ConsumerRecord<String, String> record) {
    String key = record.key();
    String value = record.value();
    int partition = record.partition();
    long offset = record.offset();
}
```

### 6.4 Consumer Group과의 관계

같은 `groupId`를 사용하는 Consumer 인스턴스들은 하나의 Consumer Group에 속한다. 파티션은 그룹 내 Consumer들에 분배된다. 일반적으로 하나의 파티션은 같은 그룹에서 한 시점에 한 Consumer에게 할당된다.

- Consumer 인스턴스 수가 파티션 수보다 적으면 한 Consumer가 여러 파티션을 처리할 수 있다.
- Consumer 인스턴스 수가 파티션 수보다 많으면 일부 인스턴스는 유휴 상태가 될 수 있다.
- 서로 다른 `groupId`는 각각 독립적으로 메시지를 소비할 수 있다.

따라서 `@KafkaListener`를 여러 개 선언했다고 무조건 메시지가 각 Listener에 복제되는 것은 아니다. Topic, group ID, 파티션 할당, Listener 설정을 함께 봐야 한다.

### 6.5 처리 성공·실패와 Offset

Listener가 메시지를 처리한 뒤 Kafka offset을 언제 커밋할지는 AckMode와 컨테이너 설정 등으로 결정된다.

- 너무 일찍 커밋하면 실제 업무 처리가 실패했는데도 메시지를 완료한 것으로 간주할 위험이 있다.
- 처리 후 커밋하는 방식은 재전달 가능성을 높이므로 중복 처리에 대비해야 한다.
- 수동 ACK를 사용할 때는 성공·실패 시점에 맞게 명확히 ACK 해야 한다.

정확한 동작은 Spring Kafka 버전, AckMode, 트랜잭션 설정, 에러 핸들러에 따라 달라진다.

### 6.6 Listener에서 반드시 고려할 사항

- 멱등성: 같은 이벤트가 재전달되어도 중복 주문·중복 재고 차감이 발생하지 않도록 한다.
- 역직렬화: Producer가 보낸 데이터 형식과 Consumer DTO가 일치해야 한다.
- 예외 처리: 재시도할 오류와 즉시 격리할 오류를 구분한다.
- Offset 커밋: 업무 처리 완료와 offset 커밋 순서를 고려한다.
- 관측성: Topic, partition, offset, event ID, correlation ID를 로그에 남긴다.

---

## 7. DLT와 DLQ

### 7.1 정의

- **DLQ(Dead Letter Queue):** 정상적인 처리 경로에서 처리할 수 없는 메시지를 별도로 보관하는 큐 또는 저장 경로를 뜻하는 일반적인 개념이다.
- **DLT(Dead Letter Topic):** Kafka에서 실패 메시지를 보관하기 위해 별도로 사용하는 Topic을 가리키는 표현이다.

Kafka에서는 보통 실패 메시지를 별도 Topic으로 보내므로 DLT라는 표현을 자주 사용한다. 프레임워크나 문서에 따라 DLQ라는 용어를 더 넓은 의미로 사용하기도 한다.

### 7.2 왜 필요한가?

Consumer가 특정 메시지를 계속 처리하지 못하면 해당 메시지에서 반복 실패가 발생할 수 있다.

예:
- 잘못된 JSON 형식
- 필수 필드 누락
- 지원하지 않는 이벤트 버전
- 업무 규칙 위반
- DB 장애 또는 외부 API 장애

모든 실패를 무한 재시도하면 Consumer 처리량이 떨어지고, 뒤의 메시지 처리까지 지연될 수 있다.

따라서 제한된 재시도 후에도 처리할 수 없는 메시지를 DLT로 보내 정상 메시지 처리 흐름과 분리한다.

### 7.3 처리 흐름

```text
Kafka 원본 Topic
      |
      v
@KafkaListener
      |
      v
메시지 처리 시도
      |
   실패 발생
      |
      v
재시도 정책 적용
      |
   ┌──┴──────────────┐
   |                 |
처리 성공         재시도 소진
   |                 |
업무 완료             v
                  DLT로 발행
                     |
                     v
              원인 분석·수정
                     |
                     v
                재처리 여부 결정
```

### 7.4 Spring Kafka 예시

Spring Kafka에서는 `DefaultErrorHandler`와 `DeadLetterPublishingRecoverer` 등을 조합해 재시도와 DLT 전송을 구성할 수 있다.

개념 예시(실제 생성자·설정은 버전에 맞춰 확인):

```java
@Bean
public DefaultErrorHandler errorHandler(
        DeadLetterPublishingRecoverer recoverer) {

    // 최초 실패 후 1초 간격으로 2회 재시도하는 예시
    FixedBackOff backOff = new FixedBackOff(1000L, 2);

    return new DefaultErrorHandler(recoverer, backOff);
}
```

```java
@Bean
public DeadLetterPublishingRecoverer recoverer(
        KafkaTemplate<Object, Object> kafkaTemplate) {

    return new DeadLetterPublishingRecoverer(
        kafkaTemplate,
        (record, exception) ->
            new TopicPartition(
                record.topic() + ".DLT",
                record.partition()
            )
    );
}
```

위 예시는 개념을 보여주는 설정이다. DLT Topic 생성, 파티션 수, Producer 직렬화, 권한, 예외 분류, 재시도 정책은 별도로 구성해야 한다.

### 7.5 재시도와 DLT를 구분하기

| 상황 | 일반적인 처리 방향 |
|---|---|
| 일시적인 DB 연결 오류 | 제한된 재시도 |
| 외부 API 일시 장애 | 지수 백오프 등 재시도 |
| JSON 역직렬화 불가 | 오류 메시지 격리 또는 별도 처리 |
| 필수 데이터 누락 | 재시도보다 DLT 격리 및 원인 수정 |
| 비즈니스 규칙 위반 | 무조건 재시도하지 않고 거절·격리 처리 |
| 재시도 횟수 초과 | DLT 전송 검토 |

모든 예외를 동일하게 재시도하면 안 된다. 일시적 오류(Transient)와 영구적 오류(Permanent)를 구분해야 한다.

### 7.6 DLT 운영 시 주의 사항

- DLT로 보냈다고 문제가 해결된 것은 아니다. 실패 원인을 확인하고 조치해야 한다.
- 원본 Topic, partition, offset, 예외 정보, 이벤트 ID 등을 보존해야 추적하기 쉽다.
- 재처리 전에는 실패 원인을 수정해야 한다. 그렇지 않으면 같은 실패가 반복된다.
- 재처리 시 중복 이벤트가 발생할 수 있으므로 Consumer 멱등성이 필요하다.
- DLT 보관 기간, 접근 권한, 모니터링, 알림, 재처리 승인 절차를 정한다.
- 재시도와 DLT 전송 중에도 메시지 순서가 달라질 수 있음을 고려한다.

---

## 8. 전체 흐름 예시: 주문 생성부터 실패 메시지 격리까지

바로픽과 같은 주문·결제·재고 시스템을 예로 든 통합 구조다.

1. 주문 서비스가 주문 데이터를 저장한다.
2. 같은 DB 트랜잭션에서 `OrderCreated` 이벤트를 Outbox 테이블에 저장한다.
3. Debezium Connector가 DB 변경 로그에서 Outbox INSERT를 감지한다.
4. Outbox Event Router SMT가 Outbox 레코드를 이벤트로 변환하고 Kafka Topic으로 라우팅한다.
5. 재고 서비스의 Spring `@KafkaListener`가 주문 이벤트를 수신한다.
6. 재고 서비스는 로컬 트랜잭션으로 재고 예약과 후속 Outbox 이벤트 저장을 수행한다.
7. Debezium이 재고 서비스의 후속 이벤트를 Kafka로 전달한다.
8. Saga 오케스트레이터 또는 이벤트 기반 서비스들이 결과를 바탕으로 다음 단계를 진행한다.
9. 어느 단계에서 영구적인 처리 실패가 발생하면 재시도 정책을 적용하고, 재시도 소진 시 해당 메시지를 DLT로 격리한다.
10. 업무 실패가 확정되면 Saga가 정의된 보상 작업을 실행한다.

주의: DLT 전송과 Saga 보상은 서로 다른 역할이다.
- DLT는 **메시지를 정상 소비 경로에서 격리하고 운영자가 조사할 수 있게 하는 메시지 처리 메커니즘**이다.
- Saga 보상은 **이미 진행된 비즈니스 작업을 취소하거나 상쇄하는 업무 로직**이다.

따라서 메시지를 DLT에 넣었다고 주문이 자동 취소되거나 재고가 자동 복구되는 것은 아니다. DLT에 격리된 메시지가 Saga의 실패 상태로 연결되고, 보상 명령이 실행되도록 별도로 설계해야 한다.

---

## 9. 핵심 비교 및 학습 체크리스트

### 9.1 핵심 비교

| 개념 | 기억할 질문 | 답 |
|---|---|---|
| Saga | 여러 서비스 작업 중간에 실패하면? | 보상 트랜잭션으로 업무 결과를 상쇄 |
| Outbox | DB는 저장됐는데 이벤트 발행이 실패하면? | DB와 Outbox를 같은 트랜잭션으로 저장 |
| Envelope | Debezium 이벤트의 봉투는 무엇인가? | before/after/source/op 등 변경 맥락을 담는 구조 |
| Unwrap | 봉투에서 무엇을 꺼내는가? | 일반적으로 after의 새 row를 추출 |
| SMT | Kafka Connect에서 무엇을 하는가? | 레코드의 key/value/header/topic 등을 변환 |
| Outbox Event Router | Outbox 행을 어떻게 이벤트로 만드는가? | Outbox 컬럼을 해석해 Topic/key/payload 등을 구성 |
| `@KafkaListener` | Spring에서 Kafka 메시지를 어디서 받는가? | Listener 메서드에서 수신·처리 |
| DLT/DLQ | 처리할 수 없는 메시지는? | 제한된 재시도 후 별도 Topic/Queue에 격리 |

### 9.2 발표 전 스스로 답할 질문

1. MSA에서 여러 서비스의 DB를 하나의 로컬 트랜잭션으로 묶기 어려운 이유는?
2. Saga의 보상 트랜잭션은 DB Rollback과 어떻게 다른가?
3. Choreography와 Orchestration의 차이는?
4. DB 저장과 Kafka 발행을 별도로 수행할 때 어떤 실패 조합이 발생하는가?
5. Outbox 테이블에 비즈니스 데이터와 이벤트를 함께 저장하는 이유는?
6. Polling Publisher와 CDC 방식의 차이는?
7. Debezium Envelope의 `before`, `after`, `source`, `op`는 각각 무엇인가?
8. Envelope와 Unwrap은 어떻게 다른가?
9. `ExtractNewRecordState`와 Outbox Event Router SMT는 어떤 목적이 다른가?
10. SMT는 Kafka Consumer 애플리케이션의 비즈니스 로직과 어떻게 다른가?
11. Spring의 `@KafkaListener`는 Consumer Group 및 파티션과 어떤 관계인가?
12. Listener 처리 실패 시 재시도와 offset commit을 어떻게 고려해야 하는가?
13. DLT와 DLQ는 어떤 의미이며, 재시도 소진 후 왜 필요한가?
14. DLT에 메시지를 보냈다고 Saga 보상까지 자동으로 실행되는가?

### 9.3 최종 암기 문장

- **Saga:** 여러 서비스의 로컬 트랜잭션을 연결하고, 실패하면 보상 작업으로 전체 업무 흐름을 수습한다.
- **Outbox:** 비즈니스 데이터와 발행할 이벤트를 같은 DB 트랜잭션으로 저장한다.
- **Envelope:** Debezium 변경 이벤트의 데이터와 변경 맥락을 감싸는 구조다.
- **Unwrap:** Envelope에서 새 row 등 필요한 데이터를 추출해 레코드를 단순화한다.
- **SMT:** Kafka Connect 레코드를 메시지 단위로 변환한다.
- **Outbox Event Router:** Outbox 행을 도메인 이벤트로 해석해 Kafka Topic 등으로 라우팅한다.
- **`@KafkaListener`:** Spring 애플리케이션에서 Kafka 메시지를 소비하는 메서드를 선언한다.
- **DLT/DLQ:** 반복 처리할 수 없는 메시지를 별도 경로에 격리해 분석·복구할 수 있게 한다.

---

## 참고 자료 및 공식 문서

아래 문서는 개념 확인과 실제 설정 시 우선 참고할 자료다. 버전에 따라 설정 속성과 클래스 사용법이 달라질 수 있으므로 프로젝트 버전의 문서를 확인한다.

1. **Microservices.io — Saga Pattern**  
   https://microservices.io/patterns/data/saga.html

2. **Microservices.io — Transactional Outbox**  
   https://microservices.io/patterns/data/transactional-outbox.html

3. **Debezium Documentation — Outbox Event Router**  
   https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html

4. **Debezium Documentation — Event Flattening (ExtractNewRecordState)**  
   https://debezium.io/documentation/reference/stable/transformations/event-flattening.html

5. **Kafka Connect Documentation — Single Message Transforms**  
   https://kafka.apache.org/documentation/#connect_transforms

6. **Spring for Apache Kafka — `@KafkaListener`**  
   https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/listener-annotation.html

7. **Spring for Apache Kafka — Handling Exceptions**  
   https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html

8. **Spring for Apache Kafka — Dead Letter Publishing Recoverer**  
   https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html

9. **AWS Prescriptive Guidance — Saga Pattern**  
   https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/saga.html
