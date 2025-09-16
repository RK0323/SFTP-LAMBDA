package com.example.lambda;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterResult;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

public class SftpToS3LambdaSSHJ implements RequestHandler<Object, String> {

    private static final String SFTP_HOST = System.getenv("SFTP_HOST");
    private static final String SFTP_USER = System.getenv("SFTP_USER");
    private static final int SFTP_PORT = Integer.parseInt(System.getenv("SFTP_PORT"));
    private static final String SFTP_FILE = System.getenv("SFTP_FILE"); 
    private static final String S3_BUCKET = System.getenv("S3_BUCKET"); 
    private static final String S3_KEY = System.getenv("S3_KEY");       

    // Name of your SSM parameter where private key is stored
    private static final String SSM_PARAM_NAME = System.getenv("SSM_PARAM_NAME");

    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Starting SFTP download and S3 upload...\n");

        SSHClient ssh = new SSHClient();
        try {
            // ✅ Fetch private key from SSM Parameter Store
            AWSSimpleSystemsManagement ssm = AWSSimpleSystemsManagementClientBuilder.defaultClient();
            GetParameterRequest req = new GetParameterRequest()
                    .withName(SSM_PARAM_NAME)
                    .withWithDecryption(true); // if you store as SecureString
            GetParameterResult result = ssm.getParameter(req);
            String privateKey = result.getParameter().getValue();

            // ✅ Write private key to /tmp
            File tempKey = File.createTempFile("sftp_key", ".pem");
            try (FileOutputStream fos = new FileOutputStream(tempKey)) {
                fos.write(privateKey.getBytes(StandardCharsets.UTF_8));
            }

            ssh.addHostKeyVerifier(new PromiscuousVerifier());
            ssh.connect(SFTP_HOST, SFTP_PORT);

            // ✅ Authenticate using private key
            ssh.authPublickey(SFTP_USER, tempKey.getAbsolutePath());

            File localFile = new File("/tmp/downloaded.csv");
            try (SFTPClient sftp = ssh.newSFTPClient()) {
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
