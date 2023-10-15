package TicTacToe;

import java.util.ArrayList;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;

public class Board {
	
	public int size;
    public PlayingPiece[][]board;

    public Board(int size) {
        this.size = size;
        board = new PlayingPiece[size][size];
    }


    public boolean addPiece(int row, int column, PlayingPiece playingPiece) {

        if(board[row][column] != null) {
            return false;
        }
        board[row][column] = playingPiece;
        return true;
    }


    public List<SimpleEntry<Integer, Integer>> getFreeCells() {
        List<SimpleEntry<Integer, Integer>> freeCells = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                if (board[i][j] == null) {
                    SimpleEntry<Integer, Integer> rowColumn = new SimpleEntry<>(i, j);
                    freeCells.add(rowColumn);
                }
            }
        }

        return freeCells;
    }

    public void printBoard() {

        for (int i = 0; i < size; i++) {
            for (int j = 0; j < size; j++) {
                if (board[i][j] != null) {
                   System.out.print(board[i][j].pieceType.name() + "   ");
                } else {
                    System.out.print("    ");

                }
                System.out.print(" | ");
            }
            System.out.println();

        }
    }

}
