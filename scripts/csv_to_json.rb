java_import org.apache.commons.io.IOUtils
java_import java.nio.charset.StandardCharsets
java_import org.apache.nifi.processor.io.StreamCallback

require 'csv'
require 'fileutils'
require 'json'
require 'logger'

class FileStreamCallback
  include StreamCallback

  def process(in_stream, out_stream)
    # read file as text
    text = IOUtils.toString(in_stream, StandardCharsets::UTF_8)

    # parse as CSV, convert to JSON using header row
    csv_data = CSV.parse(text)
    header_row = csv_data.shift
    json_data = csv_data.map {|row| Hash[header_row.zip(row)] }
    json_string = json_data.to_json

    # rewrite file as json string
    out_stream.write(json_string.to_java.getBytes(StandardCharsets::UTF_8))
  end
end

flowfile = session.get()
if flowfile.nil?
  return
end

# setup logger
log_path = '/nifi/logs/'
log_file = 'csv_to_json.log'
FileUtils.mkdir_p(log_path) unless File.directory?(log_path)
$logger = Logger.new("#{log_path}#{log_file}")

# ensure we only process json/csv files
filename = flowfile.getAttribute("filename")
if filename !~ /\.csv$/
  $logger.warn("File extension must be csv: #{filename}")
  session.transfer(flowfile, REL_FAILURE)
  return
end

begin

  stream_callback = FileStreamCallback.new
  flowfile = session.write(flowfile, stream_callback)

  # update filename extension
  new_filename = filename.gsub(/\.csv$/, '.json')
  flowfile = session.putAttribute(flowfile, 'filename', new_filename)

  # set index/type
  index_type = /^(.*)\.json$/.match(new_filename)[1].downcase
  index_type = /^(.*?)_part/.match(index_type)[1] if index_type =~ /_part/
  flowfile = session.putAttribute(flowfile, 'index_type', index_type)

  $logger.info("file: #{filename}; new file: #{new_filename}; index_type: #{index_type}")

  session.transfer(flowfile, REL_SUCCESS)

rescue => e

  $logger.error(e)

  session.transfer(flowfile, REL_FAILURE)

end
