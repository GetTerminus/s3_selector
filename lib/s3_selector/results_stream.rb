class ResultsStream
  DEFAULT_S3_SELECT_THREADS = 5
  DELIMITER = "\n".bytes.first.freeze

  attr_accessor :s3_files, :number_of_threads, :query, :s3_client, :region,
                :input_format, :input_format_options

  def initialize(
    s3_files:,
    query: 'SELECT * FROM S3Object',
    s3_client: Aws::S3::Client.new,
    number_of_threads: DEFAULT_S3_SELECT_THREADS,
    region: 'us-east-1',
    input_format: :parquet,
    input_format_options: {}
  )
    self.s3_files             = s3_files
    self.s3_client            = s3_client
    self.query                = query
    self.number_of_threads    = number_of_threads
    self.region               = region
    self.input_format         = input_format
    self.input_format_options = input_format_options
  end

  def records
    Enumerator.new do |yielder|
      if s3_files.length == 1
        read_s3_file(file: s3_files.first) { |data| yielder << data }
      else
        ConcurrentExecutor.consume_enumerable(s3_files) do |s3_file|
          read_s3_file(file: s3_file) { |data| yielder << data }
        end
      end
    end
  end

  private

  def signer
    @signer ||= Aws::Sigv4::Signer.new(
      service: 's3',
      region: region,
      credentials_provider: Aws::CredentialProviderChain.new.resolve,
      uri_escape_path: false,
    )
  end

  def s3_select_request_body
    @s3_select_request_body ||= <<~XML
      <SelectObjectContentRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Expression>#{query}</Expression>
        <ExpressionType>SQL</ExpressionType>
        <InputSerialization>
          #{input_serialization_body}
        </InputSerialization>
        <OutputSerialization>
          <JSON>
            <RecordDelimiter>\n</RecordDelimiter>
          </JSON>
        </OutputSerialization>
      </SelectObjectContentRequest>
    XML
  end

  def input_serialization_body
    case input_format
    when :parquet then '<Parquet />'
    when :json then json_input_serialization_body
    else
      raise "unknown input format #{input_format}"
    end
  end

  def json_input_serialization_body
    format = input_format_options&.fetch(:type, '')&.to_s&.upcase

    raise 'When querying JSON you must specify a input format option `:type` of either DOCUMENT or LINES' unless %w[DOCUMENT LINES].include?(format)

    compression = input_format_options&.fetch(:compression, '')&.to_s&.upcase

    <<~BODY
      #{compression && compression != '' ? "<CompressionType>#{compression}</CompressionType>" : ''}
      <JSON>
         <Type>#{format}</Type>
      </JSON>
    BODY
  end

  def decoder
    # Do NOT memoize this. Need a fresh stream decoder per HTTP request
    Aws::EventStream::Decoder.new
  end

  def bytes_to_utf8(bytes)
    bytes.pack('C*').force_encoding('utf-8')
  end

  def split_and_ensure_delimiter(buffer)
    found = []
    position = 0
    last_slice_end = 0

    while position < buffer.length
      if buffer[position] == DELIMITER
        found << buffer[last_slice_end..(position - 1)]
        last_slice_end = position += 1
      else
        position += 1
      end
    end

    [found, buffer[last_slice_end..]]
  end

  def read_s3_file(file:)
    file_request_decoder = decoder

    url = URI("#{file.public_url}?select&select-type=2")

    signature = signer.sign_request(
      http_method: 'POST',
      url: url,
      headers: { 'Content-Type' => 'application/octet-stream' },
      body: s3_select_request_body,
    )

    request = Net::HTTP::Post.new(url.request_uri, signature.headers.merge({ 'Content-Type' => 'application/octet-stream' }))
    request.body = s3_select_request_body

    mon = Monitor.new

    Net::HTTP.start(url.host, url.port, use_ssl: true) do |http|
      bytes = []
      http.request(request) do |response|
        response.read_body do |chunk|
          message, chunk_eof = file_request_decoder.decode_chunk(chunk)
          if message && message.headers[':event-type'].value == 'Records'
            lines = []

            mon.synchronize do
              bytes.concat(message.payload.read.bytes)
              lines, bytes = split_and_ensure_delimiter(bytes)
            end

            lines.each do |line|
              yield JSON.parse(bytes_to_utf8(line))
            rescue JSON::ParserError
            end
          end

          if chunk_eof && !!bytes && !bytes.empty?
            begin
              yield JSON.parse(bytes_to_utf8(bytes))
              bytes = []
            rescue JSON::ParserError
            end
          end
        end
      end
    end
  end
end
