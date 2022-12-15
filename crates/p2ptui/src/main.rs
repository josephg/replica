#![allow(unused_imports)]

use std::{io, thread, time::Duration};
use tui::{backend::CrosstermBackend, widgets::{Widget, Block, Borders}, layout::{Layout, Constraint, Direction}, Terminal, Frame};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use tui::backend::Backend;
use tui_textarea::{Input, Key, TextArea};

// fn ui<B: Backend>(f: &mut Frame<B>) {
//
// }

fn main() -> Result<(), io::Error> {
    // setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout().lock();
    // execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut textarea = TextArea::default();

    loop {
        terminal.draw(|f| {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .margin(1)
                .constraints(
                    [
                        Constraint::Percentage(20),
                        Constraint::Percentage(80),
                    ].as_ref()
                )
                .split(f.size());

            let block = Block::default()
                .title("Note list")
                .borders(Borders::from_bits(Borders::TOP.bits() | Borders::RIGHT.bits()).unwrap());
            f.render_widget(block, chunks[0]);

            // let block = Block::default()
            //     .title("Cool note")
            //     .borders(Borders::TOP);
            // f.render_widget(block, chunks[1]);
            f.render_widget(textarea.widget(), chunks[1]);
        })?;

        match crossterm::event::read()?.into() {
            Input { key: Key::Esc, .. } => break,
            input => {
                textarea.input(input);
            }
        }
    }

    // thread::sleep(Duration::from_millis(5000));

    // restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        // DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}