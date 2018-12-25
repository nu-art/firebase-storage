package com.nu.art.firebase.storage;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobInfo.Builder;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.StorageOptions;
import com.nu.art.core.exceptions.runtime.BadImplementationException;
import com.nu.art.core.exceptions.runtime.ImplementationMissingException;
import com.nu.art.core.utils.PoolQueue;
import com.nu.art.modular.core.Module;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.HashMap;

public class Module_FirebaseStorage
	extends Module {

	public interface DownloadListener {

		void onDownload(InputStream is, Throwable t)
			throws IOException;
	}

	public interface UploadListener {

		void onUpload(OutputStream os, Throwable t)
			throws IOException;
	}

	public class FirebaseBucket {

		public class UploadTransaction {

			private String relativePathInBucket;
			private String contentType;
			private BlobTargetOption[] targetOptions = {};
			private UploadListener listener;

			public UploadTransaction setRelativePathInBucket(String relativePathInBucket) {
				this.relativePathInBucket = relativePathInBucket;
				return this;
			}

			public final UploadTransaction setContentType(String contentType) {
				this.contentType = contentType;
				return this;
			}

			public final UploadTransaction execute(UploadListener listener) {
				if (listener == null)
					throw new ImplementationMissingException("MUST provide upload listener");

				this.listener = listener;

				if (!uploadQueue.isAlive())
					throw new BadImplementationException("MUST initialize the bucket first");

				uploadQueue.addItem(this);
				return this;
			}

			private void execute() {
				WriteChannel writer = null;
				try {
					Builder builder = BlobInfo.newBuilder(name, relativePathInBucket);
					if (contentType != null)
						builder.setContentType(contentType);

					BlobInfo blobInfo = builder.build();
					Blob blob = storage.create(blobInfo, targetOptions);
					writer = blob.writer();
					listener.onUpload(Channels.newOutputStream(writer), null);
				} catch (Throwable t) {
					try {
						listener.onUpload(null, t);
					} catch (Throwable e) {
						logError("Error while handling error", e);
					}
				} finally {
					if (writer != null) {
						try {
							writer.close();
						} catch (IOException ignore) {}
					}
				}
			}
		}

		public class DownloadTransaction {

			private String relativePathInBucket;
			private DownloadListener listener;

			public DownloadTransaction setRelativePathInBucket(String relativePathInBucket) {
				this.relativePathInBucket = relativePathInBucket;
				return this;
			}

			public final DownloadTransaction execute(DownloadListener listener) {
				if (listener == null)
					throw new ImplementationMissingException("MUST provide download listener");

				if (!downloadQueue.isAlive())
					throw new BadImplementationException("MUST initialize the bucket first");

				this.listener = listener;

				downloadQueue.addItem(this);
				return this;
			}

			private void execute() {
				ReadChannel reader = null;
				try {
					reader = storage.reader(name, relativePathInBucket);
					listener.onDownload(Channels.newInputStream(reader), null);
				} catch (Throwable e) {
					try {
						listener.onDownload(null, e);
					} catch (Exception e1) {
						logError("Error while handling error", e);
					}
				} finally {
					if (reader != null)
						reader.close();
				}
			}
		}

		private final String name;

		private PoolQueue<UploadTransaction> uploadQueue = new PoolQueue<UploadTransaction>() {
			@Override
			protected void executeAction(UploadTransaction transaction) {
				transaction.execute();
			}
		};

		private PoolQueue<DownloadTransaction> downloadQueue = new PoolQueue<DownloadTransaction>() {
			@Override
			protected void executeAction(DownloadTransaction transaction) {
				transaction.execute();
			}
		};

		public FirebaseBucket(String name) {
			this.name = name;
		}

		public FirebaseBucket setUploadThreadCount(int uploadThreadCount) {
			uploadQueue.createThreads("bucket-upload-" + name + "");
			return this;
		}

		public FirebaseBucket setDownloadThreadCount(int downloadThreadCount) {
			downloadQueue.createThreads("bucket-upload-" + name + "");
			return this;
		}

		public final UploadTransaction createUploadTransaction(String relativePathInBucket) {
			return new UploadTransaction().setRelativePathInBucket(relativePathInBucket);
		}

		public final DownloadTransaction createDownloadTransaction(String relativePathInBucket) {
			return new DownloadTransaction().setRelativePathInBucket(relativePathInBucket);
		}
	}

	private Storage storage;
	private HashMap<String, FirebaseBucket> buckets = new HashMap<>();
	private GoogleCredentials credentials;

	public void setCredentials(GoogleCredentials credentials) {
		this.credentials = credentials;
	}

	public void connect() {
		try {
			storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
		} catch (Throwable e) {
			throw new BadImplementationException("Unable to stare storage", e);
		}
	}

	@Override
	protected void init() {
		if (credentials != null)
			connect();
	}

	public final FirebaseBucket getOrCreateBucket(String name) {
		FirebaseBucket firebaseBucket = buckets.get(name);
		if (firebaseBucket != null)
			return firebaseBucket;

		buckets.put(name, firebaseBucket = new FirebaseBucket(name));
		return firebaseBucket;
	}
}
