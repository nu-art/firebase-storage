package com.nu.art.firebase.storage;

import org.junit.Test;

public class Test_Storage
	extends Test_StorageBase {

	@Test
	public void uploadFile() {
		AsyncTestim<Boolean> testGroup = createTestGroup();
		testGroup.addTest(uploadFileTest("sample-image.jpg", "image/jpg", "test/sample-image.jpg"));
		testGroup.execute();
	}

	@Test
	public void downloadFile() {
		AsyncTestim<Boolean> testGroup = createTestGroup();
		testGroup.addTest(downloadFileTest("sample-image.jpg", "test/sample-image.jpg"));
		testGroup.execute();
	}

	@Test
	public void checkList() {
		String[] files = {
			"sample-image-0.jpg",
			"sample-image-1.jpg",
			"sample-image-2.jpg",
			"sample-image-3.jpg"
		};

		AsyncTestim<Boolean> testGroup = createTestGroup();
		for (String file : files) {
			testGroup.addTest(uploadFileTest("sample-image.jpg", "image/jpg", "test-list/" + file));
		}

		testGroup.execute();

		AsyncTestim<Boolean> list = createTestGroup();
		list.addTest(listFiles("test-list/", files));
		list.execute();
	}
}
