# Call Bridge LE Audio 서비스

Bluetooth LE Audio 전화 브리지에서 BlueZ와 Asterisk 사이에 놓이는 프로세스
두 개입니다. [asterisk-chan-mobile-leaudio][chan],
[bluez-leaudio-server-fixes][bluez]와 합쳐지면 사슬 전체가 완성됩니다 —
휴대폰의 LE Audio 유니캐스트 스트림과 통화 제어가 Asterisk에 SIP 트렁크로
들어옵니다.

*English version: [README.md](README.md)*

**전체 사슬을 세우는 순서: [BUILDING.ko.md](BUILDING.ko.md).**

    callbridge-le-call-go/   LE Audio + 이동통신 통화 제어
    callbridge-sms-go/       MAP/MNS SMS 브리지. 양쪽이 함께 쓰는 BAP 엔드포인트와
                             LC3 인계 코드(internal/bluez)도 여기 있습니다

## 왜 필요한가

`bluetoothd`가 LE 링크와 ASE 상태 기계와 CIS를 소유합니다. Asterisk는 BAP를
다시 구현하지 않는 한 거기에 손댈 수 없습니다. 그래서 `chan_mobile`은 스트림을
직접 열지 않고, 유닉스 소켓 두 개에 접속해서 **다른 누군가가 해주기를
기다립니다.**

    /run/asterisk-leaudio/leaudio.sock    LC3 ISO 서술자 쌍
    /run/asterisk-leaudio/lecall.sock     통화 스냅숏, 명령, 결과

그 소켓을 여는 쪽이 `le-call-go`입니다. D-Bus로 BAP/CAP/TMAP 엔드포인트를
등록하고, GTBS Call State와 Call Control Point를 구독하고, 통화가 실제로
성립하면 `SCM_RIGHTS`로 ISO 파일 서술자를 넘겨줍니다.

**모듈이 스스로 만들어낼 수 없는 조각이 바로 이 인계입니다.** 이 저장소가
없으면 `asterisk-chan-mobile-leaudio`는 빌드도 되고 시험도 통과하지만 통화를
실어 나르지는 못합니다.

## le-call-go

    le-call-go -mode probe -device <본딩된-주소>    객체 조회와 제한된 GTBS 읽기
    le-call-go -mode serve -device <본딩된-주소>    엔드포인트 등록 후 서비스

두 소켓 모두 root 전용 `SOCK_SEQPACKET`이고 지정된 peer UID 하나만 받습니다.
모든 미디어 인계는 **살아 있는 통화 하나의 토큰을 반드시 들고 있어야** 하며,
다중 통화로 모호해지면 추측하지 않고 fail-closed로 멈춥니다. 이 프로세스는
미디어만 보고 통화 수락이나 종료를 유추하지 않습니다.

디버깅 전에 알아두시면 좋은 동작이 세 가지 있습니다.

**휴대폰에서 받은 통화는 휴대폰에 돌려줍니다.** LE Audio 기기가 연결돼 있으면
통화 오디오를 어디로 보낼지는 안드로이드가 결정하고, 한 번 정하면 되돌리지
않습니다. 스트림을 거절하면 같은 스트림을 다시 제시하고, 그동안 **어디에서도
소리가 나지 않습니다.** 그래서 GTBS가 이쪽 Accept 없이 통화가 `Incoming`을
벗어났다고 알리면, 브로커는 광고를 보류한 뒤 LE 링크를 끊습니다. 그러면
휴대폰이 자체 수화기로 되돌립니다.

**순서가 중요합니다.** 보류보다 끊기를 먼저 하면 disconnect 신호에 광고가
재무장되고, 약 5초 뒤 휴대폰이 그 광고에 응답해 브리지가 다시 오디오를
가져갑니다 — 그게 바로 고치려던 증상입니다. 보류를 풀 때는 광고를 명시적으로
다시 걸어야 합니다. 그 시점엔 disconnect 신호가 이미 지나갔기 때문입니다.

발신 통화는 GTBS outgoing 플래그로 제외합니다. 이쪽에서 시작한 통화이므로
Accept를 기대할 이유도, 요구할 이유도 없습니다.


**광고는 표적형이고 조건부입니다.** BAP·CAP Targeted Announcement는 휴대폰의
LE bearer가 끊겨 있는 동안에만 나갑니다. 본딩된 휴대폰이 먼저 연결을 걸어오는
설계라, `Bearer.LE1 Connected=true`가 되면 광고를 멈추고 `false`가 되면 다시
겁니다. 이쪽에서 나가는 연결은 없습니다.

**GTBS 세션의 유효성도 같은 속성에 묶여 있습니다.** 끊기는 즉시 readiness를
false로 내리고 알림·명령 상태를 비우므로, 재연결하면 낡은 GATT 세션을 재사용
하지 않고 처음부터 다시 조회합니다. 하위 서비스나 transport 경로에서 오는
신호로는 세션을 무효화할 수 없습니다.

`le-sink-nudge`는 조용해진 sink를 한 번 찔러보는 일회성 도구입니다.

## sms-go

RFCOMM 위의 MAP/MNS입니다. 휴대폰의 메시지 알림 서비스를 구독해 새 메시지를
가져오고 AMI로 Asterisk에 전달합니다. 발신은 반대로 작은 HTTP 엔드포인트를
거칩니다.

설정은 전부 환경 변수입니다 — `BT_DEVICE`, `AMI_HOST`, `AMI_USER`,
`AMI_SECRET_FILE`, `MAS_CHANNEL`, `MNS_CHANNEL`, `OUTBOX_LISTEN`,
`SMS_RECIPIENTS` 등 전체 목록은 `internal/config/config.go`에 있습니다.
**RFCOMM 채널 번호는 기종마다 다릅니다.** 가정하지 말고 조회하십시오.

SMS는 LE Audio 경로와 독립입니다. 전화 브리지만 원하셔도 이 모듈은 필요합니다
— `internal/bluez`가 여기 있기 때문입니다. 다만 `sms-bridge`를 실행할 필요는
없습니다.

## 빌드

    cd callbridge-sms-go     && go build ./...
    cd callbridge-le-call-go && go build ./...

모듈 경로가 `callbridge.local/...`인 것은 의도한 것입니다. 체크아웃에서
빌드하는 구조이고(`callbridge-le-call-go`는 `replace`로 옆 모듈을 참조합니다)
`go get`으로 받아가는 물건이 아니라서, 일부러 해석되지 않는 경로를 씁니다.

## 범위와 한계

브리지 한 대를 실제로 돌리는 코드이고, 사슬을 완성하려고 공개합니다. 안드로이드
휴대폰 한 대와 어댑터 한 개를 상대로 개발했으며, LE 경로는 그 휴대폰이 제시하는
32 kHz / 10 ms / 80 옥텟 LC3 구성을 전제합니다.

이 사본의 식별자는 전부 대체값입니다 — 시험 안의 단말 주소, 광고되는 로컬
이름, AMI 계정, 그리고 모든 전화번호가 그렇습니다. 번호는 `010-0000-0000`
대역을 쓰는데 **할당될 수 없는 번호**라서, 여기 있는 것으로는 실제 가입자에게
전화가 걸리지 않습니다.

GTBS에는 DTMF가 없습니다. Control Point 결과도 후속 Call State도 받지 못한
명령은 Asterisk의 통화 타임아웃에 맡깁니다. 프로세스가 추측하지 않습니다.

## 라이선스

GPL-2.0-only. `LICENSE`를 보십시오.

[chan]: https://github.com/YDaBang/asterisk-chan-mobile-leaudio
[bluez]: https://github.com/YDaBang/bluez-leaudio-server-fixes
