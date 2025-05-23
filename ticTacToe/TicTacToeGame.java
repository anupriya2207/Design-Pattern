package TicTacToe;

import java.util.AbstractMap.SimpleEntry;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class TicTacToeGame {
	
	  Deque<Player> players;
	  Board gameBoard;

	  public void initializeGame(){
	       players = new LinkedList<>();
	        PlayingPieceX crossPiece = new PlayingPieceX();
	        Player player1 = new Player("Player1", crossPiece);

	        PlayingPieceO noughtsPiece = new PlayingPieceO();
	        Player player2 = new Player("Player2", noughtsPiece);

	        players.add(player1);
	        players.add(player2);

	        gameBoard = new Board(3);
	  }
	  
	  public String startGame(){

	        boolean noWinner = true;
	        while(noWinner){

	            //take out the player whose turn is and also put the player in the list back
	            Player playerTurn = players.removeFirst();

	            //get the free space from the board
	            gameBoard.printBoard();
	            List<SimpleEntry<Integer, Integer>> freeSpaces =  gameBoard.getFreeCells();
	            if(freeSpaces.isEmpty()) {
	                noWinner = false;
	                continue;
	            }

	            //read the user input
	            System.out.print("Player:" + playerTurn.name + " Enter row,column: ");
	            Scanner inputScanner = new Scanner(System.in);
	            String s = inputScanner.nextLine();
	            String[] values = s.split(",");
	            int inputRow = Integer.valueOf(values[0]);
	            int inputColumn = Integer.valueOf(values[1]);
	            
	            boolean pieceAddedSuccessfully = gameBoard.addPiece(inputRow,inputColumn, playerTurn.playingPiece);
	            if(!pieceAddedSuccessfully) {
	                System.out.println("Incorredt possition chosen, try again");
	                players.addFirst(playerTurn);
	                continue;
	            }
	            players.addLast(playerTurn);

	            boolean winner = isThereWinner(inputRow, inputColumn, playerTurn.playingPiece.pieceType);
	            if(winner) {
	                return playerTurn.name;
	            }
	        }

	        return "tie";
	    }

	    public boolean isThereWinner(int row, int column, PieceType pieceType) {

	        boolean rowMatch = true;
	        boolean columnMatch = true;
	        boolean diagonalMatch = true;
	        boolean antiDiagonalMatch = true;

	        //check in row
	        for(int i=0;i<gameBoard.size;i++) {

	            if(gameBoard.board[row][i] == null || gameBoard.board[row][i].pieceType != pieceType) {
	                rowMatch = false;
	            }
	        }

	        //check in column
	        for(int i=0;i<gameBoard.size;i++) {

	            if(gameBoard.board[i][column] == null || gameBoard.board[i][column].pieceType != pieceType) {
	                columnMatch = false;
	            }
	        }

	        for(int i=0, j=0; i<gameBoard.size;i++,j++) {
	            if (gameBoard.board[i][j] == null || gameBoard.board[i][j].pieceType != pieceType) {
	                diagonalMatch = false;
	            }
	        }
	        
	        for(int i=0, j=gameBoard.size-1; i<gameBoard.size;i++,j--) {
	            if (gameBoard.board[i][j] == null || gameBoard.board[i][j].pieceType != pieceType) {
	                antiDiagonalMatch = false;
	            }
	        }

	        return rowMatch || columnMatch || diagonalMatch || antiDiagonalMatch;
	    }


}









public void uploadCsvToS3(String csvFilePath) {
    LOG.info("Uploading CSV file to S3...");

    // Define S3 bucket name and object key
    String bucketName = mercuryS3Properties.getBucket();
    String objectKey = "uploads/" + Paths.get(csvFilePath).getFileName().toString(); // S3 file path

    // Create S3 client
    S3Client s3Client = S3Client.builder()
            .region(Region.of(mercuryS3Properties.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(objectStore.getS3Keys().getAccessKey(), 
                                               objectStore.getS3Keys().getSecretKeys())))
            .build();

    try {
        // Upload file
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(objectKey)
                        .build(),
                Paths.get(csvFilePath));

        LOG.info("CSV file successfully uploaded to S3: s3://{}/{}", bucketName, objectKey);
    } catch (S3Exception e) {
        LOG.error("S3 upload failed: {}", e.getMessage(), e);
    } finally {
        s3Client.close();
    }
}
