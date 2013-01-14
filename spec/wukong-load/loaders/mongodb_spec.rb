require 'spec_helper'
require 'mongo'

describe Wukong::Load::MongoDBLoader do

  let(:loader)                   { Wukong::Load::MongoDBLoader.new                                }
  let(:loader_with_custom_database) { Wukong::Load::MongoDBLoader.new(:database    => 'custom_database')   }
  let(:loader_with_custom_collection)  { Wukong::Load::MongoDBLoader.new(:collection  => 'custom_collection') }
  let(:loader_with_custom_id)    { Wukong::Load::MongoDBLoader.new(:id_field => '_custom_id')     }

  let(:record)                     { {'text' => 'hi'                                        } }
  let(:record_with_database)          { {'text' => 'hi', '_database'          => 'custom_database'   } }
  let(:record_with_custom_database)   { {'text' => 'hi', '_custom_database'   => 'custom_database'   } }
  let(:record_with_collection)        { {'text' => 'hi', '_collection'        => 'custom_collection' } }
  let(:record_with_custom_collection) { {'text' => 'hi', '_custom_collection' => 'custom_collection' } }
  let(:record_with_id)             { {'text' => 'hi', '_id'             => 'the_id'         } }
  let(:record_with_custom_id)      { {'text' => 'hi', '_custom_id'      => 'the_id'         } }

  

  context "without an MongoDB available" do
    before do
      Mongo::MongoClient.should_receive(:new).and_raise(StandardError)
    end

    it "raises an error on setup" do
      expect { processor(:mongodb_loader) }.to raise_error(Wukong::Error)
    end
  end

  context "with a MongoDB available" do
    before do
      @client = double()
      Mongo::MongoClient.stub!(:new).and_return(@client)
    end
    
    it_behaves_like 'a processor', :named => :mongodb_loader
    
    context "routes" do
      context "all records" do
        it "to a default database" do
          loader.database_name_for(record).should == loader.database
        end
        it "to a given database" do
          loader_with_custom_database.database_name_for(record).should == 'custom_database'
        end
        it "to a default collection" do
          loader.collection_name_for(record).should == loader.collection
        end
        it "to a given collection" do
          loader_with_custom_collection.collection_name_for(record).should == 'custom_collection'
        end
      end
      
      context "records having a value for" do
        it "default database field to the given database" do
          loader.database_name_for(record_with_database).should == 'custom_database'
        end
        it "given database field to the given database" do
          loader_with_custom_database.database_name_for(record_with_custom_database).should == 'custom_database'
        end
        it "default collection field to the given collection" do
          loader.collection_name_for(record_with_collection).should == 'custom_collection'
        end
        it "given collection field to the given collection" do
          loader_with_custom_collection.collection_name_for(record_with_custom_collection).should == 'custom_collection'
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
      before do
        @collection = double()
        loader.stub!(:collection_for).and_return(@collection)
      end
      it "insert requests on a record without an ID" do
        @collection.should_receive(:insert).with(kind_of(Hash))
        loader.load({'_database' => 'foo', '_collection' => 'bar'})
      end
      
      it "update requests on a record with an ID" do
        @collection.should_receive(:update).with({:_id => '1'}, kind_of(Hash), :upsert => true).and_return({})
        loader.load({'_database' => 'foo', '_collection' => 'bar', '_id' => '1'})
      end
    end
    
  end
end
