require 'spec_helper'

describe Wukong::Load::ElasticsearchLoader do

  let(:loader)                   { Wukong::Load::ElasticsearchLoader.new                                }
  let(:loader_with_custom_index) { Wukong::Load::ElasticsearchLoader.new(:index    => 'custom_index')   }
  let(:loader_with_custom_type)  { Wukong::Load::ElasticsearchLoader.new(:es_type  => 'custom_es_type') }
  let(:loader_with_custom_id)    { Wukong::Load::ElasticsearchLoader.new(:id_field => '_custom_id')     }

  let(:record)                     { {'text' => 'hi'                                        } }
  let(:record_with_index)          { {'text' => 'hi', '_index'          => 'custom_index'   } }
  let(:record_with_custom_index)   { {'text' => 'hi', '_custom_index'   => 'custom_index'   } }
  let(:record_with_es_type)        { {'text' => 'hi', '_es_type'        => 'custom_es_type' } }
  let(:record_with_custom_es_type) { {'text' => 'hi', '_custom_es_type' => 'custom_es_type' } }
  let(:record_with_id)             { {'text' => 'hi', '_id'             => 'the_id'         } }
  let(:record_with_custom_id)      { {'text' => 'hi', '_custom_id'      => 'the_id'         } }

  it_behaves_like 'a processor', :named => :elasticsearch_loader

  context "without an Elasticsearch available" do
    before do
      Net::HTTP.should_receive(:new).and_raise(StandardError)
    end

    it "raises an error on setup" do
      expect { processor(:elasticsearch_loader) }.to raise_error(Wukong::Error)
    end
  end

  context "routes" do
    context "all records" do
      it "to a default index" do
        loader.index_for(record).should == loader.index
      end
      it "to a given index" do
        loader_with_custom_index.index_for(record).should == 'custom_index'
      end
      it "to a default type" do
        loader.es_type_for(record).should == loader.es_type
      end
      it "to a given type" do
        loader_with_custom_type.es_type_for(record).should == 'custom_es_type'
      end
    end
    
    context "records having a value for" do
      it "default index field to the given index" do
        loader.index_for(record_with_index).should == 'custom_index'
      end
      it "given index field to the given index" do
        loader_with_custom_index.index_for(record_with_custom_index).should == 'custom_index'
      end
      it "default type field to the given type" do
        loader.es_type_for(record_with_es_type).should == 'custom_es_type'
      end
      it "given type field to the given type" do
        loader_with_custom_type.es_type_for(record_with_custom_es_type).should == 'custom_es_type'
      end
    end
  end

  context "detects IDs" do
    it "based on the absence of a default ID field" do
      loader.id_for(record).should be_nil
    end
    it "based on the value of a default ID field" do
      loader.id_for(record_with_id).should == 'the_id'
    end
    it "based on the value of a custom ID field" do
      loader_with_custom_id.id_for(record_with_custom_id).should == 'the_id'
    end
  end

  context "sends" do
    it "create requests on a record without an ID" do
      loader.should_receive(:request).with(Net::HTTP::Post, '/foo/bar', kind_of(Hash))
      loader.load({'_index' => 'foo', '_es_type' => 'bar'})
    end
    
    it "update requests on a record with an ID" do
      processor(:elasticsearch_loader) do |proc|
        proc.should_receive(:request).with(Net::HTTP::Put, '/foo/bar/1', kind_of(Hash))
        proc.load({'_index' => 'foo', '_es_type' => 'bar', '_id' => '1'})
      end
    end
  end

  context "receives" do
    let(:connection) { double()                                              }
    before           { Net::HTTP.should_receive(:new).and_return(connection) }
    
    let(:ok) do
      mock("Net::HTTPOK").tap do |response|
        response.stub!(:code).and_return('200')
        response.stub!(:body).and_return('{"ok": true}')
      end
    end
    let(:created) do
      mock("Net::HTTPCreated").tap do |response|
        response.stub!(:code).and_return('201')
        response.stub!(:body).and_return('{"created": true}')
      end
    end
    let(:not_found) do
      mock("Net::HTTPNotFound").tap do |response|
        response.stub!(:code).and_return('404')
        response.stub!(:body).and_return('{"error": "Not found"}')
      end
    end
    
    context "201 Created" do
      before { connection.should_receive(:request).with(kind_of(Net::HTTP::Post)).and_return(created) }
      it "by logging an INFO message" do
        processor(:elasticsearch_loader) do |proc|
          proc.log.should_receive(:info)
          proc.load(record)
        end
      end
    end

    context "200 OK" do
      before { connection.should_receive(:request).with(kind_of(Net::HTTP::Put)).and_return(ok) }
      it "by logging an INFO message" do
        processor(:elasticsearch_loader) do |proc|
          proc.log.should_receive(:info)
          proc.load(record_with_id)
        end
      end
    end

    context "an error response from Elasticsearch" do
      before { connection.should_receive(:request).with(kind_of(Net::HTTP::Post)).and_return(not_found) }
      it "by logging an ERROR message" do
        processor(:elasticsearch_loader) do |proc|
          proc.log.should_receive(:error)
          proc.load(record)
        end
      end
    end
    
  end
end
