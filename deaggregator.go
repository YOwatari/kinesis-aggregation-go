package deaggregator

import (
	"bytes"
	"crypto/md5"
	"fmt"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/golang/protobuf/proto"
)

//refs: https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
var (
	MagicHeader = []byte{0xF3, 0x89, 0x9A, 0xC2}
)

func DeaggreateRecords(in []*kinesis.Record) ([]*kinesis.Record, error) {
	var out []*kinesis.Record

	for _, r := range in {
		// verify
		var header []byte
		var messageAndDigest []byte
		if len(r.Data) >= len(MagicHeader) {
			header = append(header, r.Data[:len(MagicHeader)]...)
			messageAndDigest = append(messageAndDigest, r.Data[len(MagicHeader):]...)
		} else {
			out = append(out, r)
			continue
		}

		if !bytes.Equal(header, MagicHeader) || len(messageAndDigest) <= md5.Size {
			out = append(out, r)
			continue
		}

		// verify
		var message []byte
		var digest []byte
		message = append(message, messageAndDigest[:len(messageAndDigest)-md5.Size]...)
		digest = append(digest, messageAndDigest[len(messageAndDigest)-md5.Size:]...)

		if fmt.Sprintf("%x", digest) != fmt.Sprintf("%x", md5.Sum(message)) {
			out = append(out, r)
			continue
		}

		ar := &AggregatedRecord{}
		if err := proto.Unmarshal(message, ar); err != nil {
			return nil, err
		}

		for _, rr := range ar.Records {
			out = append(out, &kinesis.Record{
				ApproximateArrivalTimestamp: r.ApproximateArrivalTimestamp,
				Data:           rr.GetData(),
				EncryptionType: r.EncryptionType,
				PartitionKey:   r.PartitionKey,
				SequenceNumber: r.SequenceNumber,
			})
		}
	}

	return out, nil
}
