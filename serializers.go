package plumtree

import (
	"encoding/json"
	"errors"
	"slices"
)

func Serialize(msg Message) ([]byte, error) {
	typeByte := byte(msg.Type)
	typeBytes := []byte{typeByte}
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return nil, err
	}
	return append(typeBytes, payloadBytes...), nil
}

func GetMsgType(msgBytes []byte) MessageType {
	if len(msgBytes) == 0 {
		return UNKNOWN_MSG_TYPE
	}
	msgType := MessageType(int8(msgBytes[0]))
	if !slices.Contains(KnownMsgTypes(), msgType) {
		return UNKNOWN_MSG_TYPE
	}
	return msgType
}

func GetPayload(msgBytes []byte) ([]byte, error) {
	if len(msgBytes) == 0 {
		return nil, errors.New("empty message")
	}
	return msgBytes[1:], nil
}

func Deserialize(payloadBytes []byte, payload any) error {
	return json.Unmarshal(payloadBytes, payload)
}
