.PHONY: proto

proto: $(wildcard protos/*.proto)
	@rm -rf *.pb.go
	docker run --rm -v $(CURDIR):/out -v $(CURDIR)/protos:/protos -w / grpc/go protoc --proto_path=protos --go_out=out $^
