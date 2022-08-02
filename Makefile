twirp:
	protoc --twirp_crystal_out=src/twirp_transport --crystal_out=src/twirp_transport --plugin=bin/protoc-gen-twirp_crystal --plugin=bin/protoc-gen-crystal ./protos/twirp.proto
	crystal tool format

ruby_client:
	protoc --ruby_out=clients/sourced_client.rb/lib/sourced_client --twirp_ruby_out=clients/sourced_client.rb/lib/sourced_client protos/twirp.proto
	sed -i '' 's/SourcedStore/SourcedClient/g' ./clients/sourced_client.rb/lib/sourced_client/protos/*.rb

clean:
	rm -rf clients/sourced_client.rb/lib/sourced_client/protos
