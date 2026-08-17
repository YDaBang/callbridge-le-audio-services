# 전체 사슬 세우기

네 개 층, 세 개 저장소입니다. 각 층의 내용은 해당 저장소에 있고, 여기 적는
것은 **어떤 순서로 세워야 하는지**와 **다음 단계로 넘어가기 전에 무엇을
확인해야 하는지**입니다.

*English version: [BUILDING.md](BUILDING.md)*

    1  BlueZ            bluez-leaudio-server-fixes          패치 후 설치
    2  le-call-go       이 저장소                            소켓 2개 생성
    3  chan_mobile      asterisk-chan-mobile-leaudio        그 소켓에 접속
    4  설정             chan_mobile.conf                    기기별 opt-in

순서는 취향 문제가 아닙니다. 3단계는 2단계가 만든 소켓에 접속하고, 1단계는
**두 번째 통화가 되느냐 마느냐**를 결정합니다.

## 1 — BlueZ

[bluez-leaudio-server-fixes][bluez]의 패치를 적용해 설치합니다. **223은 건너뛸
수 없습니다.** 없으면 전송 IO 참조를 들고 있는 Unicast Server가 `Releasing`을
벗어나지 못하고, 약 3.5초 뒤 휴대폰의 LE Audio 워치독이 그룹을 내립니다. 그
뒤로는 첫 통화 이후 모든 통화가 **아무 로그도 없이** 실패합니다.

**확인.** 패치가 추가하는 유닛 시험 `BAP/USR/SCC/IO-RELEASE-01`은 수정이 없으면
멈추고, 있으면 통과합니다.

## 2 — le-call-go

    cd callbridge-sms-go     && go build ./...
    cd callbridge-le-call-go && go build ./...
    le-call-go -mode probe -device <본딩된-주소>

`probe`는 읽기만 합니다. BlueZ 객체를 조회하고 제한된 GTBS 읽기만 수행하므로
`serve` 전에 돌려보십시오.

**확인.** `probe`가 본딩된 기기에서 GTBS 서비스를 정확히 하나 찾아야 합니다.
그다음 `serve`를 띄우고 소켓 두 개가 생겼는지 봅니다.

    /run/asterisk-leaudio/leaudio.sock
    /run/asterisk-leaudio/lecall.sock

없으면 여기서 멈추십시오. 다음 단계가 접속할 대상이 없습니다.

프로세스가 강제 종료되면 소켓 파일이 남고, 다음 기동은 그것을 덮어쓰기를
거부합니다. **소유자가 없음을 확인한 뒤에** 지우십시오.

## 3 — chan_mobile

[asterisk-chan-mobile-leaudio][chan] 설명대로 패치를 적용하고 신규 파일을
복사한 뒤 빌드합니다.

**확인.** 미디어 수명 주기 시험 9개가 통과해야 합니다. liblc3만 있으면 되므로
이 확인은 위 단계들과 독립입니다.

모듈을 적재하려면 어댑터 voice 설정을 **먼저** 넣어야 합니다.

    hciconfig <dev> voice 0x0060
    asterisk -rx "module load chan_mobile"

순서가 바뀌면 적재가 실패하고 브리지가 안 올라옵니다.

## 4 — 설정

    audiotransport=le-canary
    callcontrol=le-gtbs

기기별로 **둘 다** 넣어야 합니다. 하나만 넣으면 전화는 받아지는데 소리가 안
나옵니다. 둘 다 기본값이 `classic`이므로, 설정하지 않은 기기는 이전과 똑같이
동작합니다.

## 휴대폰이 먼저 연결을 걸어옵니다

브로커는 휴대폰의 LE bearer가 끊겨 있는 동안에만 BAP·CAP Targeted Announcement를
내보내고, 연결되면 멈춥니다. 이쪽에서 나가는 연결은 없습니다.

그래서 `le-call-go`를 재시작하면 원래는 휴대폰이 붕 뜹니다. 링크는
`bluetoothd`가 소유하므로 휴대폰은 재시작을 보지 못한 채 그룹을 ACTIVE로
유지하고 CIS를 더 이상 제공하지 않습니다 — **통화는 연결되는데 소리도 에러도
없는** 상태입니다.

**이건 브로커가 알아서 처리합니다.** 기동 시점에 LE가 이미 붙어 있으면 bearer를
끊고, 속성이 실제로 false가 되는 것을 확인한 뒤 광고를 다시 겁니다. 휴대폰이
연결이 끊긴 것을 관측하고 상태를 다시 세우게 됩니다. **손으로 할 일은
없습니다.**

## 통화는 붙는데 소리가 없을 때

1. **설정 두 줄 중 하나만 넣었을 때.** LE 미디어에 클래식 통화 제어, 또는 그
   반대.
2. **BlueZ가 패치되지 않았고** 직전 통화가 ASE를 `Releasing`에 남겼을 때. 즉
   지금이 첫 통화가 아니라 두 번째일 때입니다.
3. **휴대폰이 클래식을 쓰고 있을 때.** 프로파일 목록 말고 **협상된 코덱**을
   보십시오. 휴대폰이 HFP를 고르고 있는 것으로 확인되면
   `android/profile-policy-helper`로 그 기기에 한해 HFP를 금지할 수 있습니다.

셋 다 에러를 남기지 않습니다. 그게 어려운 지점입니다.

[bluez]: https://github.com/YDaBang/bluez-leaudio-server-fixes
[chan]: https://github.com/YDaBang/asterisk-chan-mobile-leaudio
