# frozen_string_literal: true

require 'spec_helper'
require 's3_selector/results_stream'

RSpec.describe S3Selector::ResultsStream do
  before :all do
    @random_seed = ENV.fetch('S3_SELECT_RANDOM_SEED', (100 * rand).to_i)
    puts "Running S3 Select with seed: #{@random_seed}"
    @randomizer = Random.new(@random_seed)
  end

  def randomly_slice_string(str)
    pos = 0
    pieces = []
    while pos < str.length
      # Move at least one character to a random length
      next_pos = pos + 1 + (str.length - pos) * @randomizer.rand * 0.5
      pieces << str[pos..(next_pos - 1)]
      pos = next_pos
    end
    pieces
  end

  let(:client) { double(Aws::S3::Client) }
  describe 's3_select_request_body' do
    describe 'the default behavior' do
      it 'streams in parquet' do
        instance = described_class.new(s3_client: client, s3_files: [])
        expect(instance.send(:s3_select_request_body)).to include '<Parquet />'
      end
    end

    describe 'streaming in json' do
      it 'streams in json' do
        instance = described_class.new(
          s3_client: client,
          s3_files: [],
          input_format: :json,
          input_format_options: { type: :lines },
        )
        body = instance.send(:s3_select_request_body)

        expect(body.gsub(/\s+/, '')).to include '<JSON><Type>LINES</Type></JSON>'
      end

      it 'streams in gziped json' do
        instance = described_class.new(
          s3_client: client,
          s3_files: [],
          input_format: :json,
          input_format_options: { compression: :gzip, type: :lines },
        )
        body = instance.send(:s3_select_request_body)

        expect(body.gsub(/\s+/, '')).to include '<CompressionType>GZIP</CompressionType><JSON><Type>LINES</Type></JSON>'
      end

      it 'throws an error if you do not specify type' do
        instance = described_class.new(
          s3_client: client,
          s3_files: [],
          input_format: :json,
          input_format_options: {},
        )
        expect { instance.send(:s3_select_request_body) }.to raise_error(/DOCUMENT or LINES/)
      end

      it 'throws an error if you specify an unknown type' do
        instance = described_class.new(
          s3_client: client,
          s3_files: [],
          input_format: :json,
          input_format_options: { type: :foobar },
        )
        expect { instance.send(:s3_select_request_body) }.to raise_error(/DOCUMENT or LINES/)
      end
    end
  end

  describe 'when streaming content' do
    let(:file1_key) { 'foo/bar/baz' }
    let(:file1_bucket) { 'omg' }
    let(:file1_url) { "https://#{file1_bucket}.s3.amazonaws.com/#{file1_key}?select&select-type=2" }
    let(:decoder) { Aws::EventStream::Decoder.new }
    let(:msg1_eof) { true }
    let(:msg2_eof) { true }
    before do
      WebMock.disable_net_connect!(allow: "https://#{file1_bucket}.s3.amazonaws.com")

      allow(instance).to receive(:signer) do
        double(
          sign_request: double(
            headers: {},
          ),
        )
      end

      allow(instance).to receive(:decoder) { decoder }
      allow(decoder).to receive(:decode_chunk).and_return(
        [
          double(
            headers: {
              ':event-type' => double(value: 'Records'),
            },
            payload: StringIO.new(message1_payload),
          ),
          msg1_eof,
        ],
        [
          double(
            headers: {
              ':event-type' => double(value: 'Records'),
            },
            payload: StringIO.new(message2_payload),
          ),
          msg2_eof,
        ],
      )
    end

    context 'when streaming a single file' do
      let(:message2_payload) { '' }
      let(:message1_payload) do
        <<~ENDFILE
          {"value": 1}
          {"value": 2}
          {"value": 3}
        ENDFILE
      end

      let(:instance) do
        described_class.new(
          s3_client: client,
          s3_files: [
            double(bucket: double(name: file1_bucket), key: file1_key, public_url: file1_url),
          ],
        )
      end

      it 'streams all records' do
        expect(ConcurrentExecutor).not_to receive(:consume_enumerable)

        expect(instance.records.to_a).to match_array([
                                                       { 'value' => 1 },
                                                       { 'value' => 2 },
                                                       { 'value' => 3 },
                                                     ])
      end

      context 'json split across multiple messages' do
        let(:message1_payload) do
          <<~ENDFILE[0..-2] # Need to snip the new line off the last line
            {"value": 1}
            {"value": 2}
            {"val
          ENDFILE
        end

        let(:message2_payload) do
          <<~ENDFILE
            ue": 3}
            {"value2": 4}
            {"value2": 5}
            {"value2": 6}
          ENDFILE
        end

        xit 'returns parsed records' do
          expect(instance.records.to_a).to match_array([
                                                         { 'value' => 1 },
                                                         { 'value' => 2 },
                                                         { 'value' => 3 },
                                                         { 'value2' => 4 },
                                                         { 'value2' => 5 },
                                                         { 'value2' => 6 },
                                                       ])
        end
      end
    end

    context 'when streaming multiple files' do
      let(:instance) do
        described_class.new(
          s3_client: client,
          s3_files: [
            double(bucket: double(name: file1_bucket), key: file1_key, public_url: file1_url),
            double(bucket: double(name: file2_bucket), key: file2_key, public_url: file2_url),
          ],
        )
      end

      let(:message1_payload) do
        <<~ENDFILE
          {"value": 1}
          {"value": 2}
          {"value": 3}
        ENDFILE
      end

      let(:file2_key) { 'why/not' }
      let(:file2_bucket) { 'dude' }
      let(:file2_url) { "https://#{file2_bucket}.s3.amazonaws.com/#{file2_key}?select&select-type=2" }
      let(:message2_payload) do
        <<~ENDFILE
          {"value2": 4}
          {"value2": 5}
          {"value2": 6}
        ENDFILE
      end

      describe '#split_and_ensure_delimiter' do
        it 'splits on delimiter' do
          expect(instance.send(:split_and_ensure_delimiter, "asdf\n1234\nqwerty".bytes)).to eq(
            [
              ['asdf'.bytes, '1234'.bytes],
              'qwerty'.bytes,
            ],
          )
        end
      end

      describe 'yields lines' do
        context 'no newline at end' do
          let(:message1_payload) do
            <<~ENDFILE[0...-1]
              {"value": 1}
              {"value": 2}
              {"value": 3}
            ENDFILE
          end

          it 'streams all records' do
            WebMock.allow_net_connect!
            expect(instance.records.to_a).to match_array([
                                                           { 'value' => 1 },
                                                           { 'value' => 2 },
                                                           { 'value' => 3 },
                                                           { 'value2' => 4 },
                                                           { 'value2' => 5 },
                                                           { 'value2' => 6 },
                                                         ])
            WebMock.disable_net_connect!
          end
        end

        it 'streams all records' do
          WebMock.allow_net_connect!
          expect(instance.records.to_a).to match_array([
                                                         { 'value' => 1 },
                                                         { 'value' => 2 },
                                                         { 'value' => 3 },
                                                         { 'value2' => 4 },
                                                         { 'value2' => 5 },
                                                         { 'value2' => 6 },
                                                       ])
          WebMock.disable_net_connect!
        end
      end
    end
  end
end
