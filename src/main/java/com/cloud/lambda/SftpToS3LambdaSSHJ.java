package com.example.lambda;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

public class SftpToS3LambdaSSHJ implements RequestHandler<Object, String> {

    private static final String SFTP_HOST = System.getenv("SFTP_HOST");
    private static final String SFTP_USER = System.getenv("SFTP_USER");
    private static final int SFTP_PORT = Integer.parseInt(System.getenv("SFTP_PORT"));
    private static final String SFTP_FILE = System.getenv("SFTP_FILE"); // Remote file path in SFTP

    private static final String S3_BUCKET = System.getenv("S3_BUCKET"); // Target S3 bucket
    private static final String S3_KEY = System.getenv("S3_KEY");       // Key (path) in S3

    // Hardcoded PKCS#8 private key
    private static final String PRIVATE_KEY = """
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCvXJjgCCuKB3uj
sbBwWQo1CRkDsGFO5IPp2IB1BsW3F/kN+yib5BYRwkmddDCuvYL+k1Uyunh614mI
R8PcGrfu048Elhn3gXQi+cizmeKyXwAzt3XCEw4PXnyq2jwzKrdVjhhX6kh84TZ8
4G6P49cSCTvhLUpNRNH0U28QZfkoJYnQL2MWKCHSwCzpUde+rfctHAZRwIduJlii
H/Xq/wreFxAQVpT/DsjKOMUGE+YhzAgnpzK1E1Zxdj+/DMaOPo0wfn2LlWl8+5se
NU8laWNTSPvNLwhc8EA3Fv2MS0388g7c2jSV8f+i/cAX6b95ujjvR1XL+uopJJ9O
/A8VhQehAgMBAAECggEACui6xe0cMEkI4wUR0dtoLCZ1vOoDE7JZlnmWfDFAqYQ/
RSRjwVbuHIq2o6nvwV8ewBGQUkiRaCryblzwjUfIO35DvmhMEz6lZ+ucFbg0Bx6r
14vJMpqW3FPnc4WCCzjUEOl3EDkN617jUd9wYVkuLvwOEpMLtUTbvtRV63I8jse0
lfwe4AsB5NZpK4RgtivMgVHqff+cXPRLEzXHC6jvt1bapo8B/6mEl2yzmAQ1voc5
/u6DJg7qLelz/tU936YsKOGyTqJw/GWBlW9hbSbDLBDcg6427tJiCpois+mV00VX
vfkyz8HzJ8qt/zSllIar1mjUYvSekx5sLQh5OESN8QKBgQDnFfYdIaUClUpZv+ee
Vv3pzuo5ibkDZynbXcH3TK6zR9HXGGje3fFfMykmMlFKZEYJ7x++YCvqZS5i8MfD
xt3mxRdYOE74enhSz7h2SH/LfVdRGEq0afaKBp6TjRJqn0VM/SkasPYO/HWlmGsN
uktmk7yDqS7rehFBZitdDxYJBQKBgQDCRKJU87QKVx9scJX5s0GfabjACkSGsRwC
OjRI/6Q1KPpQLOIPYGBE7HBRXEfd0CybsP5gQ/JFINYnQhrFUCJPc0LHHhtT4oVr
KNCuokDqWq7B99grspg0ls8cEep6c2PbGJWcsQIJR+pilvA8ANh6AVUFgz5uF498
lfPnymtW7QKBgCPxxJCPdS1Lr376XLrCaUh/COveQksHNvmKQeuHn/z/BOLiRx2z
hCH8wT/Rv9dEdaiB63wfVXVmO6rkLQ6E1mtY6OKuD4qS8jhfWx/68vXiDzzr/GLH
wLODiBMHdH/lOlBZaJVgRUXbeylA5hYCjkvsWvxXypaSLDDyBXi6KgFJAoGBAKzT
4pcxODr6xznx73r1vqvTokk22T/60LAql9lZlPy2N/qb84E6fGrU7hdzwXvUd9sK
20NJL3/QdrT2Wlqvr9Z+9Cmw3x8BpzBuXzj9oELd6hsmI2q8uCwQ/rA6QCAwB+Yj
OPbtgqI/GIYdQhwfpHbmOH1vBVtsGnr77MGJk3UBAoGAS3P/aT00k5GWv4RCysGG
eF8pFjfCluRek7vR/ERjjhA9tIy37IMPoYGyFFO1Mjf7Uf51E9tAVq2NgI8Di8W0
Y9/OzxmHzCGy2Xv9eWVWY9AYOibtEo0lHoqxtawFsQC0U8y1T07yBMmrK7CPE6Zm
Jcm0yen3PRn30qI35Kix/8E=
-----END PRIVATE KEY-----
""";

    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Starting SFTP download and S3 upload...\n");

        SSHClient ssh = new SSHClient();
        try {
            // Write private key to /tmp
            File tempKey = File.createTempFile("sftp_key", ".pem");
            try (FileOutputStream fos = new FileOutputStream(tempKey)) {
                fos.write(PRIVATE_KEY.getBytes(StandardCharsets.UTF_8));
            }

            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(SFTP_HOST, SFTP_PORT);

            // Authenticate using private key
            ssh.authPublickey(SFTP_USER, tempKey.getAbsolutePath());

            File localFile = new File("/tmp/downloaded.csv");

            try (SFTPClient sftp = ssh.newSFTPClient()) {
                // ✅ Download file from SFTP to Lambda /tmp
                context.getLogger().log("Downloading from SFTP: " + SFTP_FILE + "\n");
                sftp.get(SFTP_FILE, localFile.getAbsolutePath());
            }

            // ✅ Upload downloaded file to S3
            AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
            s3.putObject(S3_BUCKET, S3_KEY, localFile);

            ssh.disconnect();
            context.getLogger().log("Transfer complete! Uploaded to S3 bucket: " + S3_BUCKET + "/" + S3_KEY + "\n");
            return "Success";

        } catch (Exception e) {
            context.getLogger().log("Transfer failed: " + e.getMessage());
            e.printStackTrace();
            return "Failed: " + e.getMessage();
        } finally {
            try { ssh.close(); } catch (Exception ignore) {}
        }
    }
}
