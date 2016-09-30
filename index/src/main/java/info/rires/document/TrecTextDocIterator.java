package info.rires.document;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

import java.io.*;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TrecTextDocIterator implements Iterator<Document> {

    protected BufferedReader br;
    protected boolean at_eof = false;

    public TrecTextDocIterator(BufferedReader br) throws FileNotFoundException {
        this.br = br;
    }

    @Override
    public boolean hasNext() {
        return !at_eof;
    }

    @Override
    public Document next() {
        Document doc = new Document();
        StringBuffer sb = new StringBuffer();
        try {
            String line;
            Pattern docno_tag = Pattern.compile("<DOCNO>\\s*(\\S+)\\s*<");
            boolean in_doc = false;
            while (true) {
                line = this.br.readLine();
                if (line == null) {
                    at_eof = true;
                    break;
                }
                if (!in_doc) {
                    if (line.startsWith("<DOC>")) {
                        in_doc = true;
                    }
                    else {
                        continue;
                    }
                }
                if (line.startsWith("</DOC>")) {
                    in_doc = false;
                    sb.append(line);
                    break;
                }

                Matcher m = docno_tag.matcher(line);
                if (m.find()) {
                    String docno = m.group(1);
                    doc.add(new StringField("docno", docno, Field.Store.YES));
                }

                sb.append(line);
            }
            if (sb.length() > 0) {
                doc.add(new TextField("contents", sb.toString(), Field.Store.NO));
            }

        } catch (IOException e) {
            doc = null;
        }
        return doc;
    }

    @Override
    public void remove() {
        // Do nothing, but don't complain
    }

}