package com.nu.art.firebase.storage;

import com.google.auth.oauth2.GoogleCredentials;
import com.nu.art.core.exceptions.runtime.BadImplementationException;
import com.nu.art.core.tools.FileTools;
import com.nu.art.core.tools.StreamTools;
import com.nu.art.firebase.storage.Module_FirebaseStorage.DownloadListener;
import com.nu.art.firebase.storage.Module_FirebaseStorage.FirebaseBucket;
import com.nu.art.firebase.storage.Module_FirebaseStorage.FirebaseBucket.DownloadTransaction;
import com.nu.art.firebase.storage.Module_FirebaseStorage.FirebaseBucket.UploadTransaction;
import com.nu.art.firebase.storage.Module_FirebaseStorage.UploadListener;
import com.nu.art.modular.core.ModulesPack;
import com.nu.art.modular.tests.ModuleManager_TestClass;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Test_Storage
	extends ModuleManager_TestClass {

	private static final String outputPath = "build/test/output";
	private static final String resPath = "src/test/res";
	private static FirebaseBucket bucket;

	static class Pack
		extends ModulesPack {

		Pack() {
			super(Module_FirebaseStorage.class);
		}

		@Override
		protected void init() {
			try {
				String pathToCreds = resPath + "/dev-server-key.json";
				GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(new File(pathToCreds)));
				getModule(Module_FirebaseStorage.class).setCredentials(credentials);
			} catch (IOException e) {
				throw new BadImplementationException("unable to load credential file for storage test");
			}
		}
	}

	@BeforeClass
	@SuppressWarnings("unchecked")
	public static void setUp() {
		initWithPacks(Pack.class);
		bucket = getModule(Module_FirebaseStorage.class).getOrCreateBucket("test-fcm-fdcdc.appspot.com");
		bucket.setUploadThreadCount(3);
		bucket.setDownloadThreadCount(3);
	}

	@Before
	public final void deleteOutput()
		throws IOException {
		FileTools.delete(new File(outputPath));
	}

	@Test
	public final void uploadFile() {
		final Object lock = new Object();

		final File origin = new File(resPath, "sample-image.jpg");
		final UploadTransaction transaction = bucket.createUploadTransaction("test/sample-image.jpg");
		transaction.setContentType("image/jpg");
		transaction.execute(new UploadListener() {
			@Override
			public void onUpload(OutputStream os, Throwable t)
				throws IOException {
				if (t != null) {
					logError("Error uploading file", t);
					synchronized (lock) {
						lock.notify();
					}
					return;
				}

				try {
					StreamTools.copy(origin, os);
					synchronized (lock) {
						lock.notify();
					}
				} catch (IOException e) {
					throw e;
				}
			}
		});

		synchronized (lock) {
			try {
				lock.wait();
			} catch (InterruptedException e) {
				logError(e);
			}
		}
	}

	@Test
	public final void downloadFile() {

		final Object lock = new Object();
		DownloadTransaction downloadTransaction = bucket.createDownloadTransaction("test/sample-image.jpg");
		downloadTransaction.execute(new DownloadListener() {
			@Override
			public void onDownload(InputStream is, Throwable t)
				throws IOException {
				if (t != null) {
					logError("Error uploading file", t);
					synchronized (lock) {
						lock.notify();
					}
					return;
				}

				try {
					StreamTools.copy(is, new File(outputPath, "sample-image.jpg"));
					synchronized (lock) {
						lock.notify();
					}
				} catch (IOException e) {
					throw e;
				}
			}
		});

		synchronized (lock) {
			try {
				lock.wait();
			} catch (InterruptedException e) {
				logError(e);
			}
		}
	}
}
