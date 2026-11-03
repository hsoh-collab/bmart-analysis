### 이미지 1 (1-8.png)

**[헤더]**
- Spring Cloud & Microservices
- 8 / 20
- PART 1 Monolithic Architecture에서 MSA로

**[제목]** Scale Out — 서버 인스턴스 수를 늘리는 방법

**[부제]** Scale Out은 동일한 서버를 여러 대 구성하여 트래픽을 분산 처리하는 방식입니다.

**[① 단일 서버 구성 (Before)]**
- 하나의 서버에서 모든 요청을 처리합니다.
- 구성: Client(노트북, 휴대폰, 데스크톱 아이콘) → Server A → Database (하나의 DB)
- Server A 내부: Spring Boot Application (Monolithic)
  - User
  - Product
  - Order
  - Payment

**[② 다중 서버 구성 (After)]**
- 여러 대의 서버(인스턴스)를 추가하여 트래픽을 분산합니다.
- 구성: Client(노트북, 휴대폰, 데스크톱 아이콘) → Load Balancer → Server A / Server B / Server C → Database (공유 DB)
- Server A, Server B, Server C 각각: Spring Boot Application (Monolithic)
- 세 서버 모두 하나의 Database (공유 DB)로 연결됩니다.

**[Scale Out의 특징]**
1. 동일한 애플리케이션을 여러 대 실행한다.
2. Load Balancer가 트래픽을 분산한다.
3. 서버 장애 시 다른 인스턴스가 처리할 수 있다.
4. 비교적 구성과 운영이 단순하다.
5. 수평 확장(Horizontal Scaling)이라고 부른다.

**[주의할 점]**
1. 모든 기능이 함께 복제된다.
2. 특정 기능만 선택적으로 확장할 수 없다.
3. 세션 관리, 캐시, 파일 저장소 등을 별도로 고려해야 한다.
4. 서버 수가 많아질수록 운영 비용이 증가한다.
5. 애플리케이션 구조가 크면 배포 시간도 길어진다.

**[트래픽 분산 방식]**
- Load Balancer는 여러 인스턴스에 요청을 분산할 수 있습니다.

| 방식 | 설명 |
|---|---|
| Round Robin | 순차적으로 분산 |
| Least Connection | 연결 수가 적은 서버로 |
| IP Hash | 클라이언트 IP 기준 |
| Weighted | 가중치 기반 분산 |

**[트래픽 증가에 따른 인스턴스 확장]**
- 1대 (초기) → 3대 (트래픽 증가) → 6대 (더 많은 트래픽)

**[핵심 정리]**
- Scale Out은 서버 인스턴스 수를 늘려 처리 성능을 높이는 방식입니다.
- Monolithic 구조에서는 모든 기능이 함께 확장됩니다.
- 특정 기능에만 트래픽이 집중되는 경우 비효율적입니다.
- 이러한 한계를 해결하기 위해 Microservices Architecture가 필요합니다.

**[푸터]** Spring Cloud & Microservices | PART 1 — Monolithic Architecture에서 MSA로

---

### 이미지 2 (1-9.png)

**[헤더]**
- Spring Cloud & Microservices
- 9 / 20
- PART 1 Monolithic Architecture에서 MSA로

**[제목]** Monolithic의 Scale Out는 비효율적이다

**[부제]** Monolithic 구조에서 Scale Out을 하면, 한 기능의 트래픽 증가만으로도 전체 Application이 함께 복제되어 비효율적입니다.

**[특정 기능의 트래픽이 증가한 상황]**
- 구성: Client(노트북, 휴대폰, 데스크톱 아이콘) → (빨간 화살표) 트래픽 증가 (예: 상품 조회) → Spring Boot Application → Database (하나의 DB)
- Spring Boot Application 내부:
  - User
  - Product
  - Order
  - Payment
  - Inventory
- 하단 설명: Product 기능의 트래픽만 증가했지만, Monolithic 구조에서는 전체 Application이 하나로 동작합니다.

**[Scale Out을 적용한 결과]**
- 구성: Client → Load Balancer → Spring Boot Application (Instance 1 / Instance 2 / Instance 3) → Database (하나의 DB)
- 각 인스턴스 내부: User, Product, Order, Payment, Inventory
- 하단 설명: Product 기능만 확장하고 싶어도, 전체 Application을 여러 개의 인스턴스로 복제해야 합니다.

**[Monolithic Scale Out의 문제점]**
1. 특정 기능만 선택적으로 확장할 수 없다.
2. 모든 기능이 함께 복제되어 자원이 낭비된다.
3. 서로 트래픽 패턴이 다른 기능도 동일한 인스턴스에서 동작한다.
4. 배포, 모니터링, 장애 처리 범위가 커진다.
5. 트래픽이 증가하지 않는 기능도 불필요하게 확장된다.

**[비유로 이해하기]**
- [그림: "마트" 간판이 달린 건물]
- 상품 코너가 바빠졌다고 마트 전체를 복사해서 새 매장을 열어야 하는 것과 같습니다.
- 한 코너만 넓히고 싶은데, 건물 전체를 새로 지어야 하는 비효율적인 구조입니다.

**[핵심 포인트]**
1. Monolithic의 Scale Out은 수평 확장이지만, 기능 단위 확장이 아니다.
2. 하나의 기능 때문에 전체 Application이 복제된다.
3. 클라우드 환경에서 비용이 크게 증가할 수 있다.
4. 기능별 트래픽 특성이 다를 경우 비효율적이다.
5. 이러한 한계가 Microservices로 전환해야 하는 주요 이유 중 하나이다.

**[푸터]** Spring Cloud & Microservices | PART 1 — Monolithic Architecture에서 MSA로

---

### 이미지 3 (1-16.png)

**[헤더]**
- Spring Cloud & Microservices
- 16 / 20
- PART 1 Monolithic Architecture에서 MSA로

**[제목]** MSA 전환 전략 – 서비스 디스커버리 (Service Discovery)

**[부제]** 동적으로 변화하는 서비스의 위치를 자동으로 찾아주는 기술입니다.

**[① 서비스 디스커버리란?]**
- 서비스가 시작되면 자기 정보를 등록하고, 클라이언트는 서비스 이름으로 위치를 찾아 호출합니다.
- 다이어그램:
  - Client(노트북, 휴대폰, 데스크톱 아이콘) → Service Registry (예: Eureka): "서비스 이름으로 요청 (user-service)"
  - Service Registry (예: Eureka) → Client: "서비스 위치 정보 (IP:Port)"
  - User Service (10.0.1.11:8080), Product Service (10.0.1.12:8080), Order Service (10.0.1.13:8080) ↔ Service Registry: "서비스 등록 (주기적 갱신)"

**[② 동작 흐름]**
- 서비스 등록 → 서비스 검색 → 서비스 호출의 과정을 자동으로 처리합니다.
1. **서비스 등록 (Register)**: 각 서비스가 시작될 때 자신의 정보(서비스 이름, IP, Port)를 서비스 레지스트리에 등록합니다.
2. **서비스 검색 (Discovery)**: 클라이언트가 서비스 이름으로 레지스트리에 서비스 위치 정보를 요청합니다.
3. **서비스 호출 (Invoke)**: 레지스트리가 반환한 위치 정보로 실제 서비스에 요청을 전달합니다.

**[③ Spring Cloud Netflix Eureka 예시]**
- 가장 널리 사용되는 서비스 디스커버리 구현체입니다.
- 다이어그램:
  - Eureka Server (서비스 레지스트리)
  - User Service (유레카 클라이언트) → Eureka Server: "서비스 등록 (Heartbeat)"
  - Product Service (유레카 클라이언트) → Eureka Server: "서비스 등록 (Heartbeat)"
  - 가운데 레이블: "서비스 이름으로 호출 / 서비스 이름으로 호출 (product-service)" [불명확: 두 레이블이 겹쳐 보임]
- Eureka Server 설정 (application.yml):

```yaml
server:
  port: 8761

spring:
  application:
    name: eureka-server

eureka:
  client:
    register-with-eureka: false   # 자신은 등록하지 않음
    fetch-registry: false         # 레지스트리 정보도 가져오지 않음
```

**[④ Eureka 클라이언트 설정 (서비스 등록)]**
- 각 서비스는 Eureka에 등록하기 위한 설정을 추가합니다.
- Service 클라이언트 설정 (application.yml):

```yaml
server:
  port: 8080

spring:
  application:
    name: user-service     # 서비스 이름 (다른 서비스가 사용할 이름)

eureka:
  client:
    service-url:
      defaultZone: http://localhost:8761/eureka/   # Eureka 서버 주소
```

**[⑤ 클라이언트에서 서비스 호출 (DiscoveryClient)]**
- 서비스 이름으로 실제 인스턴스를 찾아 동적으로 호출할 수 있습니다.
- DiscoveryClient를 이용한 서비스 호출

```java
@Service
public class UserServiceClient {

    private final DiscoveryClient discoveryClient;
    private final RestTemplate restTemplate;

    public UserServiceClient(DiscoveryClient discoveryClient) {
        this.discoveryClient = discoveryClient;
        this.restTemplate = new RestTemplate();
    }

    public String callUserService() {
        List<ServiceInstance> instances =
            discoveryClient.getInstances("user-service");

        ServiceInstance instance = instances.get(0);
        String url = instance.getUri().toString() + "/api/users";
        return restTemplate.getForObject(url, String.class);
    }
}
```

**[핵심 정리]**
1. 서비스 디스커버리는 MSA에서 필수적인 기술입니다.
2. 서비스가 시작되면 레지스트리에 자신의 정보를 등록합니다.
3. 클라이언트는 서비스 이름으로 위치를 조회하여 호출합니다.
4. Eureka 외에도 Consul, Nacos 같은 대안이 있습니다.
5. 서비스 인스턴스의 장애나 확장에도 유연하게 대응할 수 있습니다.

**[주의할 점]**
1. 레지스트리의 가용성이 매우 중요합니다.
2. 네트워크 장애 시 호출 실패에 대한 대비가 필요합니다.
3. 보안 설정과 접근 제어를 반드시 고려해야 합니다.
4. 운영 환경에서는 모니터링과 알림을 함께 구성해야 합니다.

**[하단 강조 문구]** 서비스 디스커버리는 동적인 서비스 환경에서 안정적인 통신을 가능하게 하는 기반 기술입니다.

**[푸터]** Spring Cloud & Microservices | PART 1 — Monolithic Architecture에서 MSA로

---

### 이미지 4 (2-15.png)

**[헤더]**
- Spring Cloud로 배우는 MSA
- 실무 기반 마이크로서비스 아키텍처
- PART 2. Microservice 설계 원칙
- 15 / 24

**[제목]** 15 마이크로서비스 간 통신 방법
**[부제]** 동기 통신과 비동기 통신의 이해와 선택 기준

**[인용]** “통신 방식의 선택이 시스템의 성능과 안정성을 결정한다.”

**[소개문]** 마이크로서비스는 서비스 간 통신을 통해 비즈니스 기능을 구현합니다. 상황에 맞는 통신 방식을 선택해야 안정적이고 확장 가능한 시스템을 만들 수 있습니다.

**[1. 서비스 간 통신 방식 개요]**
- 마이크로서비스 간 통신은 크게 동기 통신과 비동기 통신으로 나눌 수 있습니다.
- **동기 통신 (Synchronous)** [아이콘: 양방향 화살표]
  - 요청 후 응답을 기다림
  - 실시간 처리에 적합
  - 대표 기술: REST API, gRPC
  - 호출한 서비스가 응답을 받아야 다음 동작 수행
- **비동기 통신 (Asynchronous)** [아이콘: 메시지 봉투]
  - 요청 후 응답을 기다리지 않음
  - 이벤트 기반 처리에 적합
  - 대표 기술: Kafka, RabbitMQ
  - 메시지를 브로커에 전달하고 자신의 작업을 계속 수행

**[2. 동기 통신 (REST API)]**
- 가장 일반적인 방식으로 HTTP를 사용하여 요청/응답 방식으로 통신합니다.
- 다이어그램: Order Service (주문) → "1. HTTP 요청 (GET /products/1)" → Product Service (상품); Product Service (상품) → "2. HTTP 응답 (상품 정보)" (점선) → Order Service (주문)
- **장점**
  - 구현이 쉽고 직관적
  - 실시간 응답이 필요한 경우에 적합
- **단점**
  - 호출한 서비스가 응답을 기다려야 하므로 지연 발생
  - 장애 전파 가능성

**[3. 동기 통신 (gRPC)]**
- gRPC는 HTTP/2 기반의 고성능 RPC 프레임워크로, 프로토콜 버퍼(Protocol Buffers)를 사용하여 효율적인 서비스 간 통신을 제공합니다.
- 다이어그램: Order Service (주문) → "1. gRPC 요청 (GetProductRequest)" → Product Service (상품); Product Service (상품) → "2. gRPC 응답 (GetProductResponse)" (점선) → Order Service (주문)
- **gRPC의 특징** [아이콘: 톱니바퀴]
  - HTTP/2 기반
  - 프로토콜 버퍼 사용 (작은 메시지 크기)
  - 높은 성능과 낮은 지연 시간
  - 다양한 언어 지원
  - 실시간 스트리밍 지원 (Server/Client/Bi-directional)

**[4. 비동기 통신 (메시지 큐)]**
- 이벤트를 메시지로 발행(Publish)하고, 다른 서비스가 이를 구독(Subscribe)하여 처리하는 방식입니다.
- 다이어그램: Order Service (주문) → "1. 이벤트 발행 (주문 생성됨)" → Message Broker (Kafka / RabbitMQ) → "2. 이벤트 구독 및 처리" → Inventory Service (재고)
- 이벤트 메시지: `{ orderId: 1001, .. }`

**[비동기 통신의 장점과 단점]**
- **장점**
  - 서비스 간 느슨한 결합(비동기 처리)
  - 장애 격리 (한 서비스 장애가 다른 서비스에 직접 영향 없음)
  - 대량 트래픽 처리에 유리
  - 높은 확장성과 유연성
- **단점**
  - 구현 복잡도 증가
  - 메시지 전달 보장(중복, 순서) 고려
  - 최종 일관성(Eventual Consistency) 모델 필요
  - 실시간 응답이 필요한 경우에는 부적합

**[5. 통신 방식 선택 기준]**
- 서비스의 특성과 요구사항에 따라 적절한 통신 방식을 선택해야 합니다.

| 구분 | 동기 통신 (REST/gRPC) | 비동기 통신 (메시지 큐) |
|---|---|---|
| 응답 방식 | 요청 후 응답 대기 | 요청 후 즉시 반환 |
| 실시간성 | 높음 | 낮음 (비동기 처리) |
| 결합도 | 강한 결합 | 느슨한 결합 |
| 장애 전파 | 발생 가능 | 격리 가능 |
| 적합한 경우 | 조회, 실시간 처리 | 이벤트 처리, 대량 트래픽 |
| 대표 기술 | REST, gRPC | Kafka, RabbitMQ |

**[핵심 정리]** [아이콘: 과녁]
- 서비스 간 통신은 동기와 비동기의 특성을 이해하고, 비즈니스 요구사항에 맞게 선택해야 합니다.
- 올바른 통신 방식의 선택이 안정적이고 확장 가능한 마이크로서비스를 만듭니다.

---

### 이미지 5 (3-7.png)

**[헤더]**
- 7 / 24
- PART 3
- SPRING CLOUD MICROSERVICES

**[제목]** IP/Port 하드코딩의 문제와 동적 Instance 환경
**[부제]** Microservice에서는 Service의 IP와 Port가 동적으로 변경됩니다.

**[인용 박스]** “특정 Service의 IP와 Port를 코드에 하드코딩하면, Instance가 변경될 때마다 코드를 수정하고 다시 배포해야 합니다. Microservice는 Scale Out / Scale In / 장애 복구 과정에서 IP와 Port가 항상 동적으로 변한됩니다.”

**[① 하드코딩 방식]**
- IP와 Port를 코드에 직접 작성
- RestTemplate 사용 예시

```java
String url = "http://10.0.1.10:8080/products/1";
RestTemplate restTemplate = new RestTemplate();
Product product = restTemplate.getForObject(
    url,
    Product.class
);
```

- [경고 아이콘]
  - IP나 Port가 변경되면 코드 수정이 필요
  - 새로운 Instance가 추가되어도 알 수 없음
  - 자동화된 확장(Scale Out)에 대응 불가능

**[② Instance가 변경되는 상황]**
- Microservice는 동적으로 생성되고 종료된다.
- 다이어그램: Client (Order Service) → 
  - Product Service Instance A 10.0.1.10:8080 [실행 중]
  - Product Service Instance B 10.0.1.11:8080 [실행 중]
  - Product Service Instance C 10.0.1.12:8080 [실행 중]
  - ...
- 하단 설명: Instance는 언제든지 새로 생성되거나 종료될 수 있으며, IP와 Port가 동적으로 변경됩니다.

**[③ 동적으로 변경되는 이유]**
- 다음과 같은 상황에서 IP와 Port가 바뀝니다.
- Scale Out: 트래픽 증가로 새로운 Instance가 추가될 때
- Scale In: 트래픽 감소로 불필요한 Instance가 종료될 때
- 장애 발생: 장애가 발생한 Instance가 종료되고, 새로운 Instance가 생성될 때
- Container 재시작: Docker / Kubernetes 환경에서 재시작될 때 (새로운 IP가 할당될 수 있음)
- 배포(Deploy): 새로운 버전으로 배포하면서 Instance가 교체될 때

**[④ 하드코딩 방식의 문제점]**
- 운영 환경에서는 치명적인 문제가 발생한다.
- IP/Port 변경 시마다 코드 수정 필요
- 서비스가 여러 개인 경우 관리가 매우 어려움
- 배포 주기가 길어짐
- 장애 발생 시 빠른 복구가 어려움
- 자동 확장(Scale Out)에 대응 불가능
- 개발, 테스트, 운영 환경별로 코드가 달라질 수 있음

**[⑤ 실제 운영 환경 예시]**
- 같은 Service라도 시간이 지나면 다른 IP를 가질 수 있다.
- 처음 실행: Product Service 10.0.1.10:8080
- → Scale Out (새 인스턴스 추가): 10.0.1.10:8080, 10.0.1.11:8080
- → 재배포 (기존 인스턴스 교체): 10.0.1.21:8080, 10.0.1.22:8080

**[⑥ 해결 방법의 방향]**
- 하드코딩이 아닌 동적인 방식이 필요하다.
- Service의 위치를 동적으로 찾는 방법 (Service Discovery)
- 실행 중인 여러 Instance 중 하나를 선택하는 방법 (Load Balancing)
- 논리적인 이름으로 Service에 접근하는 방법 (예: http://product-service)
- 코드 수정 없이 Instance 변경에 자동으로 대응

**[하단 문구]** "Microservice에서는 IP와 Port를 코드에 하드코딩하지 않는다."

**[우측 하단]** DYNAMIC SERVICES NEED DYNAMIC SOLUTIONS WITH SPRING CLOUD

---

### 이미지 6 (3-8.png)

**[헤더]**
- 8 / 24
- PART 3
- SPRING CLOUD MICROSERVICES

**[제목]** Service Discovery와 Eureka Server
**[부제]** 서비스의 위치를 동적으로 찾기 위한 서비스 등록과 조회 메커니즘입니다.

**[인용 박스]** “Service Discovery는 실행 중인 Service Instance의 위치(IP와 Port)를 동적으로 찾아주는 기능이며, Eureka Server는 이를 위한 Service Registry 역할을 수행합니다.”

**[① Service Discovery란?]**
- 서비스의 위치를 동적으로 찾는 기능
- Service Name으로 위치 조회
- IP와 Port를 직접 알 필요 없음
- Instance가 추가/삭제되어도 자동 반영
- 동적인 환경(클라우드, 컨테이너)에 필수적
- 다이어그램: Order Service http://product-service → Service Discovery (Eureka) → 10.0.1.10:8080 / 10.0.1.11:8080 / 10.0.1.12:8080

**[② Eureka Server의 역할]**
- Service Registry (서비스 등록소)
- Service Instance의 등록 (Registration)
- Service Instance 목록을 저장 (Registry)
- 다른 서비스가 Instance 위치를 조회할 수 있도록 제공 (Discovery)
- Instance의 상태를 주기적으로 확인 (Heartbeat)
- [강조 박스] Eureka Server는 서비스들의 주소록과 같습니다.

**[③ 전체 동작 흐름]**
- 등록 → 유지(Heartbeat) → 조회
- 다이어그램:
  - Eureka Server (Service Registry): product-service • 10.0.1.10:8080 • 10.0.1.11:8080 • 10.0.1.12:8080 ...
  - Product Service (Instance) 10.0.1.10:8080 → Eureka Server: "1. 등록 (Registration)"
  - Product Service ↔ Eureka Server: "2. 상태 유지 (Heartbeat)"
  - Eureka Server → Order Service (Client) http://product-service: "3. 조회 (Discovery)", "Instance 목록 반환"

**[④ Eureka Client 설정 (서비스 등록)]**
- 각 서비스는 Eureka Client로 동작하며, 실행 시 자동으로 Eureka Server에 등록됩니다.
- application.yml

```yaml
spring:
  application:
    name: product-service     # 서비스 이름
  cloud:
    discovery:
      enabled: true
eureka:
  client:
    service-url:               # Eureka Server 주소
      defaultZone: http://localhost:8761/eureka
```

**[⑤ 다른 서비스에서 조회 (Eureka Client)]**
- 다른 서비스는 서비스 이름으로 Eureka Server에서 Instance 목록을 조회할 수 있습니다.
- Eureka Client 동작 예시

```java
// RestTemplate, OpenFeign, WebClient 등에서
// 서비스 이름을 사용하면 Eureka가 실제 주소를 찾아줍니다.

@RestController
public class OrderController {

    private final RestTemplate restTemplate;

    public String callProduct() {
        // "product-service" 라는 이름으로 호출
        return restTemplate.getForObject(
            "http://product-service/products", String.class);
    }
}
```

**[⑥ Eureka의 주요 특징]**
- 서비스 이름 기반의 등록 및 조회
- 동적으로 변경되는 Instance 관리
- 주기적인 Heartbeat를 통한 상태 확인
- 장애가 발생한 Instance 자동 제거
- 클라우드 환경에 적합한 구조
- Spring Cloud 생태계와 긴밀하게 연동

**[핵심 포인트]**
- 코드에 IP와 Port를 하드코딩하지 않는다.
- 서비스 이름으로 접근한다.
- Eureka가 실제 실행 중인 Instance를 찾아준다.
- 동적인 환경에서도 안정적으로 서비스에 접근할 수 있다.

**[하단 문구]** "Service Discovery는 변화하는 환경에서도 서비스를 연결해주는 핵심 기술입니다."

**[우측 하단]** DYNAMIC ENVIRONMENTS REQUIRE DYNAMIC DISCOVERY WITH SPRING CLOUD

---

### 이미지 7 (3-9.png)

**[헤더]**
- 9 / 24
- PART 3
- SPRING CLOUD & MICROSERVICES

**[제목]** Service Discovery와 Load Balancing의 차이
**[부제]** Service Discovery는 ‘어디에 있는지 찾는 것’, Load Balancing은 ‘어떤 Instance로 보낼지 선택하는 것’입니다.

**[인용 박스]** “Service Discovery가 서비스의 위치(IP:Port 목록)를 알려주면, Load Balancer가 그 목록 중에서 적절한 Instance를 선택하여 Request를 전달합니다.”

**[① Service Discovery]**
- 서비스의 위치를 찾는 기능
- 어떤 서비스의 Instance들이 어디에 있는지(IP:Port 목록)를 조회하고 관리하는 기능입니다.
- 다이어그램:
  - Product Service (Instance A), Product Service (Instance B), Product Service (Instance C) → 각각 "1. 등록 (Register)" → Eureka Server (Service Registry) → 저장된 Instance 목록 (Service Registry): product-service • 10.0.1.10:8080 • 10.0.1.11:8080 • 10.0.1.12:8080
- Service Discovery의 역할: 서비스 이름으로 현재 실행 중인 Instance들의 위치를 찾아준다.

**[② Load Balancing]**
- 여러 Instance 중 하나를 선택하는 기능
- Service Discovery에서 받은 Instance 목록 중에서 어떤 Instance로 Request를 보낼지 선택하는 기능입니다.
- 다이어그램:
  - Client (or API Gateway) → "1. 서비스 이름으로 요청 (product-service)" → Load Balancer (Spring Cloud LoadBalancer)
  - Load Balancer ↔ Eureka Server (Service Registry): "3. Instance 목록 조회"
  - Load Balancer → "3. 하나의 Instance 선택 (알고리즘)" → 10.0.1.10:8080 / 10.0.1.11:8080 / 10.0.1.12:8080
- Load Balancing의 역할: 여러 Instance 중에서 적절한 하나를 선택하여 Request를 전달한다.

**[③ 비교 정리]**

| 항목 | Service Discovery | Load Balancing |
|---|---|---|
| 역할 | 서비스의 위치(IP:Port) 조회 | 여러 Instance 중 하나를 선택 |
| 주요 구성 요소 | Eureka Server (Service Registry) | Spring Cloud LoadBalancer |
| 작동 시점 | 서비스 시작 시 등록, 실행 중 조회 | 실제 Request 호출 시 선택 |
| 결과 | Instance 목록 (IP:Port 리스트) | 선택된 하나의 Instance |
| 비유 | 전화번호부 | 여러 지점 중 하나로 연결해주는 교환원 |

**[④ 함께 동작하는 관계]**
1. 서비스가 Eureka에 자신의 정보를 등록한다.
2. Client(또는 다른 서비스)가 서비스 이름으로 요청한다.
3. LoadBalancer가 Eureka에서 Instance 목록을 조회한다.
4. LoadBalancer가 알고리즘에 따라 하나의 Instance를 선택한다.
5. 선택된 Instance로 HTTP Request를 전달한다.

**[⑤ 핵심 포인트]**
- Service Discovery와 Load Balancing은 서로 다른 역할이다.
- Discovery는 ‘어디에 있는지 찾는 것’이다.
- Load Balancing은 ‘어디로 보낼지 선택하는 것’이다.
- 두 기능이 함께 동작해야 안정적인 Microservices 통신이 가능하다.
- Spring Cloud는 Discovery와 Load Balancing을 쉽게 사용할 수 있는 기능을 제공한다.

**[하단 문구]** "Service Discovery가 길을 알려주고, Load Balancer가 그중 하나의 길을 선택한다."

**[우측 하단]** FIND SERVICES CHOOSE THE BEST INSTANCE WITH SPRING CLOUD
