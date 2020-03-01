package com.asiainfo.dacp.dp.tools.file;

import java.io.File;
import java.io.FileInputStream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * ZIP形式的压缩/解压缩,支持加密
 * @author run[zhangqi@lianchuang.com]
 * 3:42:36 PM Mar 18, 2010
 */
public class ZipFileUtil {
	
	public static int bufferSize = 2048;
	
	private static ZipFileUtil SINGLE = new ZipFileUtil();
	
	public static synchronized ZipFileUtil getInstance() {

		if (SINGLE == null) {
			SINGLE = new ZipFileUtil();			
		}
		return SINGLE;
	}
	
	/**
	 * 压缩文件
	 * @param inputFile
	 * @param zipFilename
	 * @throws IOException
	 */
	public void zip(String inputFilename, String zipFilename) throws IOException {
		zip(new File(inputFilename), zipFilename , "");
	}
	
	public void zip(String inputFilename, String zipFilename, String root) throws IOException {
		zip(new File(inputFilename), zipFilename , root);
	}
	
	/**
	 * 压缩文件
	 * @param inputFile
	 * @param zipFilename
	 * @param root
	 * @throws IOException
	 */
	public void zip(File inputFile, String zipFilename, String root) throws IOException {
		ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zipFilename));
		try {
			zip(inputFile, out, root == null ? "" : root);	
		}catch (IOException e) {   
            throw e;   
        }
		finally { 
            out.close();   
        }
	}
	
	
	/**
	 *  
	 * @param inputFile
	 * @param out
	 * @param root 压缩根目录
	 * @throws IOException
	 */
	private void zip(File inputFile, ZipOutputStream out, String root) throws IOException {
		if (inputFile.isDirectory()) {
			File[] inputFiles = inputFile.listFiles();
			out.putNextEntry(new ZipEntry(root + "/"));
			root = root.length() == 0 ? "" : root + "/";
			for (int i = 0; i < inputFiles.length; i++) {
				//递归putNextEntry
				System.out.println(root + inputFiles[i].getName());
				zip(inputFiles[i], out, root + inputFiles[i].getName());
			}

		} else {
			if (root.length() > 0) {
				out.putNextEntry(new ZipEntry(root));
			} else {
				System.out.println(inputFile.getName());
				out.putNextEntry(new ZipEntry(inputFile.getName()));
			}

			FileInputStream in = new FileInputStream(inputFile);
			try {
				int c;
				byte[] by = new byte[bufferSize];
				while ((c = in.read(by)) != -1) {
					out.write(by, 0, c);
				}
			} catch (IOException e) {
				throw e;
			} finally {
				in.close();
			}
		}
	}
	
	/**
	 * 解压缩文件
	 * @param directory
	 * @param zipFile
	 * @throws IOException
	 */
	public void unZip(String directory, String zipFile) throws IOException {
		ZipInputStream zis = null;
		try {
			zis = new ZipInputStream(new FileInputStream(zipFile));
			File unZipFile = new File(directory);
			if(!unZipFile.exists()){
				unZipFile.mkdirs();
			}			
			UnZip(zis, unZipFile);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			zis.close();
		}
	}
	
	/**
	 * 解压缩文件
	 * @param zis
	 * @param file
	 * @throws Exception
	 */
	private void UnZip(ZipInputStream zis, File file) throws Exception {
		ZipEntry zip = zis.getNextEntry();
		if (zip == null) {
			return;
		}
		String name = zip.getName();
		File unZip = new File(file.getAbsolutePath() + File.separator + name);
		if (zip.isDirectory()) {
			unZip.mkdirs();
			UnZip(zis, file);
		} else {
			unZip.createNewFile();
			FileOutputStream fos = new FileOutputStream(unZip);
			byte b[] = new byte[bufferSize];
			int c;
			while ((c = zis.read(b)) != -1) {
				fos.write(b, 0, c);
			}
			fos.close();
			//递归解析ZipEntry
			UnZip(zis, file);
		}
	}
	
	
	
	
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		ZipFileUtil.getInstance().unZip("D:\\sc_huadan_20111025", "D:\\sc_huadan_20111025.zip");
	}

}
