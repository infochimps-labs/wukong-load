module Wukong

  # Represents a line from a file.
  class FileLine
    
    include Gorillib::Model

    field :path,    String,  :doc => "Path of the original file"
    field :number,  Integer, :doc => "Line number within the original file"
    field :content, String,  :doc => "Content of the line"

  end
end
