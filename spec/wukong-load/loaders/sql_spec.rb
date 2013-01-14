require 'spec_helper'
require 'mongo'
require 'mysql2'

describe Wukong::Load::SQLLoader do

  let(:loader)                   { Wukong::Load::SQLLoader.new                                }
  let(:loader_with_custom_database) { Wukong::Load::SQLLoader.new(:database    => 'custom_database')   }
  let(:loader_with_custom_table)  { Wukong::Load::SQLLoader.new(:table  => 'custom_table') }
  let(:loader_with_custom_id)    { Wukong::Load::SQLLoader.new(:id_field => '_custom_id')     }

  let(:record)                     { {'text' => 'hi'                                        } }
  let(:record_with_database)          { {'text' => 'hi', '_database'          => 'custom_database'   } }
  let(:record_with_custom_database)   { {'text' => 'hi', '_custom_database'   => 'custom_database'   } }
  let(:record_with_table)        { {'text' => 'hi', '_table'        => 'custom_table' } }
  let(:record_with_custom_table) { {'text' => 'hi', '_custom_table' => 'custom_table' } }
  let(:record_with_id)             { {'text' => 'hi', '_id'             => 'the_id'         } }
  let(:record_with_custom_id)      { {'text' => 'hi', '_custom_id'      => 'the_id'         } }

  

  context "without an SQL available" do
    before do
      Mysql2::Client.should_receive(:new).and_raise(StandardError)
    end

    it "raises an error on setup" do
      expect { processor(:sql_loader) }.to raise_error(Wukong::Error)
    end
  end

  context "with a SQL available" do
    before do
      @client = double()
      def @client.escape record ; record.to_s ; end
      Mysql2::Client.stub!(:new).and_return(@client)
    end
    
    it_behaves_like 'a processor', :named => :sql_loader
    
    context "routes" do
      context "all records" do
        it "to a default database" do
          loader.setup
          loader.database_name_for(record).should == '`wukong`'
        end
        it "to a given database" do
          loader_with_custom_database.setup
          loader_with_custom_database.database_name_for(record).should == '`custom_database`'
        end
        it "to a default table" do
          loader.setup
          loader.table_name_for(record).should == '`streaming_record`'
        end
        it "to a given table" do
          loader_with_custom_table.setup
          loader_with_custom_table.table_name_for(record).should == '`custom_table`'
        end
      end
      
      context "records having a value for" do
        it "default database field to the given database" do
          loader.setup
          loader.database_name_for(record_with_database).should == '`custom_database`'
        end
        it "given database field to the given database" do
          loader_with_custom_database.setup
          loader_with_custom_database.database_name_for(record_with_custom_database).should == '`custom_database`'
        end
        it "default table field to the given table" do
          loader.setup
          loader.table_name_for(record_with_table).should == '`custom_table`'
        end
        it "given table field to the given table" do
          loader_with_custom_table.setup
          loader_with_custom_table.table_name_for(record_with_custom_table).should == '`custom_table`'
        end
      end
    end

    context "detects IDs" do
      it "based on the absence of a default ID field" do
        loader.setup
        loader.id_for(record).should be_nil
      end
      it "based on the value of a default ID field" do
        loader.setup
        loader.id_for(record_with_id).should == '"the_id"'
      end
      it "based on the value of a custom ID field" do
        loader_with_custom_id.setup
        loader_with_custom_id.id_for(record_with_custom_id).should == '"the_id"'
      end
    end

    context "sends" do
      before do
        loader.setup
      end
      it "insert requests on a record without an ID" do
        @client.should_receive(:query).with(%Q{INSERT INTO `foo`.`bar` (`age`, `email`, `name`) VALUES (58, "jerry@nbc.com", "Jerry Seinfeld") ON DUPLICATE KEY UPDATE `age`=58, `email`="jerry@nbc.com", `name`="Jerry Seinfeld"})
        loader.load({'_database' => 'foo', '_table' => 'bar', 'name' => 'Jerry Seinfeld', 'email' => 'jerry@nbc.com', 'age' => 58})
      end
      
      it "update requests on a record with an ID" do
        @client.should_receive(:query).with(%Q{UPDATE `foo`.`bar` SET `age`=58, `email`="jerry@nbc.com", `name`="Jerry Seinfeld" WHERE `id`="1"})
        loader.load({'_database' => 'foo', '_table' => 'bar', '_id' => '1', 'name' => 'Jerry Seinfeld', 'email' => 'jerry@nbc.com', 'age' => 58})
      end
    end
    
  end
end
