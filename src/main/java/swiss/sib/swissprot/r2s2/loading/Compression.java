/*******************************************************************************
 * Copyright (c) 2022 Eclipse RDF4J contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *******************************************************************************/
package swiss.sib.swissprot.r2s2.loading;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ProcessBuilder.Redirect;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

import org.tukaani.xz.XZInputStream;

import com.github.luben.zstd.ZstdInputStream;

import net.jpountz.lz4.LZ4FrameInputStream;

public enum Compression {

	LZ4(".lz4") {

		@Override
		public InputStream decompress(File f) throws FileNotFoundException, IOException {
			return new BufferedInputStream(new LZ4FrameInputStream(new BufferedInputStream(new FileInputStream(f))));
		}

		@Override
		public ProcessBuilder decompressInExternalProcessBuilder(File f) throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("lz4", "-qdcf", f.getAbsolutePath());
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}
	},
	GZIP(".gz") {

		@Override
		public InputStream decompress(File f) throws FileNotFoundException, IOException {
			return new GZIPInputStream(new FileInputStream(f));
		}

		@Override
		public ProcessBuilder decompressInExternalProcessBuilder(File f) throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("gunzip", "-c");
			pb.redirectInput(f);
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}
	},
	XZ(".xz") {

		@Override
		public InputStream decompress(File f) throws FileNotFoundException, IOException {
			return new XZInputStream(new FileInputStream(f));
		}

		@Override
		public ProcessBuilder decompressInExternalProcessBuilder(File f) throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("xz", "-cdf", f.getAbsolutePath());
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}
	},
	ZSTD(".zstd") {

		@Override
		public InputStream decompress(File f) throws FileNotFoundException, IOException {
			return new ZstdInputStream(new FileInputStream(f));
		}

		@Override
		public ProcessBuilder decompressInExternalProcessBuilder(File f) throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("zstd", "--format=zstd", "-cqdk", "-T0", f.getAbsolutePath());
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}
	},
	NONE("") {

		@Override
		public InputStream decompress(File f) throws FileNotFoundException, IOException {
			return new FileInputStream(f);
		}

		@Override
		public ProcessBuilder decompressInExternalProcessBuilder(File f) throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("cat");
			pb.redirectInput(f);
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}
	},
	BZIP2(".bz2") {

		@Override
		public InputStream decompress(File f) throws FileNotFoundException, IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public ProcessBuilder decompressInExternalProcessBuilder(File f) throws FileNotFoundException, IOException {
			ProcessBuilder pb = new ProcessBuilder("bunzip2");
			pb.redirectInput(f);
			pb.redirectError(Redirect.INHERIT);
			return pb;
		}
	};

	private static final ExecutorService execs = Executors
			.newFixedThreadPool(Math.max(1, Runtime.getRuntime().availableProcessors() / 2));

	private final String extension;

	private Compression(String extension) {
		this.extension = extension;
	}

	public String extension() {
		return extension;
	}

	public abstract InputStream decompress(File f) throws FileNotFoundException, IOException;

	public final Process decompressInExternalProcess(File f) throws FileNotFoundException, IOException {
		return decompressInExternalProcessBuilder(f).start();
	}

	public abstract ProcessBuilder decompressInExternalProcessBuilder(File f) throws FileNotFoundException, IOException;

	public final Process decompressInExternalProcess(File f, File to) throws FileNotFoundException, IOException {
		ProcessBuilder pb = decompressInExternalProcessBuilder(f);
		pb.redirectOutput(to);
		return pb.start();
	}

	public static String removeExtension(String name) {
		for (Compression c : values()) {
			if (!c.extension.isEmpty() && name.endsWith(c.extension())) {
				return name.substring(0, name.length() - c.extension.length());
			}
		}
		return name;
	}

	
}
