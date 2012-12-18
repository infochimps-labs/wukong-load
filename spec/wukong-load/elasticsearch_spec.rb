require 'spec_helper'

describe Wukong::Load::ElasticsearchLoader do

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
      expect { processor(:elasticsearch_loader).setup }.to raise_error(Wukong::Error)
    end
  end

  context "routes" do
    context "all records" do
      it "to a default index" do
        proc = processor(:elasticsearch_loader)
        proc.index_for(record).should == proc.index
      end
      it "to a given index" do
        processor(:elasticsearch_loader, :index => 'custom_index').index_for(record).should == 'custom_index'
      end
      it "to a default type" do
        proc = processor(:elasticsearch_loader)
        proc.es_type_for(record).should == proc.es_type
      end
      it "to a given type" do
        processor(:elasticsearch_loader, :es_type => 'custom_es_type').es_type_for(record).should == 'custom_es_type'
      end
    end
    
    context "records having a value for" do
      it "default index field to the given index" do
        processor(:elasticsearch_loader).index_for(record_with_index).should == 'custom_index'
      end
      it "given index field to the given index" do
        processor(:elasticsearch_loader, :index_field => '_custom_index').index_for(record_with_custom_index).should == 'custom_index'
      end
      it "default type field to the given type" do
        processor(:elasticsearch_loader).es_type_for(record_with_es_type).should == 'custom_es_type'
      end
      it "given type field to the given type" do
        processor(:elasticsearch_loader, :es_type_field => '_custom_es_type').es_type_for(record_with_custom_es_type).should == 'custom_es_type'
      end
    end
  end

  context "detects IDs" do
    it "based on the absence of a default ID field" do
      processor(:elasticsearch_loader).id_for(record).should be_nil
    end
    it "based on the value of a default ID field" do
      processor(:elasticsearch_loader).id_for(record_with_id).should == 'the_id'
    end
    it "based on the value of a custom ID field" do
      processor(:elasticsearch_loader, :id_field => '_custom_id').id_for(record_with_custom_id).should == 'the_id'
    end
  end

  context "having made a connection to the database" do
    
    let(:connection) { double()                         }
    let(:log)        { double()                         }
    subject          { processor(:elasticsearch_loader) }
    before           do
      Net::HTTP.should_receive(:new).and_return(connection)
      subject.stub!(:log).and_return(log)
    end
    

    context "sends" do
      it "create requests on a record without an ID" do
        subject.should_receive(:request).with(Net::HTTP::Post, '/foo/bar', kind_of(Hash))
        subject.load({'_index' => 'foo', '_es_type' => 'bar'})
      end
      it "update requests on a record with an ID" do
        subject.should_receive(:request).with(Net::HTTP::Put, '/foo/bar/1', kind_of(Hash))
        subject.load({'_index' => 'foo', '_es_type' => 'bar', '_id' => '1'})
      end
    end

    context "receives" do
      
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
          log.should_receive(:info)
          subject.load(record)
        end
      end

      context "200 OK" do
        before { connection.should_receive(:request).with(kind_of(Net::HTTP::Put)).and_return(ok) }
        it "by logging an INFO message" do
          log.should_receive(:info)
          subject.load(record_with_id)
        end
      end

      context "an error response from Elasticsearch" do
        before { connection.should_receive(:request).with(kind_of(Net::HTTP::Post)).and_return(not_found) }
        it "by logging an ERROR message" do
          log.should_receive(:error)
          subject.load(record)
        end
      end
      
    end
  end
end
