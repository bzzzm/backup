# encoding: utf-8
# require 'backup/cloud_io/cloud_files'
require 'fog'

module Backup
  module Storage
    class Google < Base
      include Storage::Cycler
      class Error < Backup::Error; end

      ##
      # Google credentials 
      # Sign up here and get your credentials here under the section "Interoperable Access".
      attr_accessor :access_key_id, :secret_access_key

      ##
      # Storage bucket
      attr_accessor :bucket

      ##
      # Region of the specified storage bucket
      attr_accessor :region

      def initialize(model, storage_id = nil)
      	super

        @path ||= 'backup'
        @path = @path.sub(/^\//, '')

        check_configuration
        connect_account
        raise Error, "Cannot verify #{ bucket }" if verify_bucket.nil?

      end

      def transfer!
        package.filenames.each do |filename|
          src = File.join(Config.tmp_path, filename)
          dest = File.join(remote_path, filename)
          Logger.info "Storing '#{ bucket }/#{ dest }'..."
          @bucket_dir.files.create(
            :key => dest,
            :body => File.read(src),
            :public => false
          )
        end
      end

      def remove!(package)

      	@conn.delete_object(@bucket, remote_path_for(package) + "/" + package.basename)
      end

    	def check_configuration
    		required = %w{ access_key_id secret_access_key bucket }
        raise Error, <<-EOS if required.map {|name| send(name) }.any?(&:nil?)
          Configuration Error
          #{ required.map {|name| "##{ name }"}.join(', ') } are all required
        EOS
    	end
      
      def connect_account
        @conn ||=begin
          @conn = Fog::Storage.new({
            :provider                         => 'Google',
            :google_storage_access_key_id     => @access_key_id,
            :google_storage_secret_access_key => @secret_access_key
          })
        end
      end

      def verify_bucket
        @conn.directories.each do |d|
          if d.key == @bucket
            @bucket_dir = d
            return true
          end

          return nil
        end
    	end


    end
  end
end